import os
import time
import pytest
import requests
import json
from pykafka import KafkaClient
from pykafka.exceptions import KafkaException
from contextlib import contextmanager

from dockertest.utils.shell import LocalShell, SSHShell, NoSuchCommandError

SWITCHMAN_IP = os.environ.get('SWITCHMAN_IP', False)
IMAGE_NAME = "memsql_spark_connector/dockertest:latest"
REAL_ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../..'))
ROOT_PATH = '/storage/testroot'
MEMSQL_OPS_ROOT = "/var/lib/memsql-ops"
SPARK_ROOT = os.path.join(MEMSQL_OPS_ROOT, "data/spark/install")
MEMSQL_JAR_PATH = os.path.join(ROOT_PATH, "memsql-spark/interface/memsql_spark_interface.jar")

def is_psyduck():
    return "SWITCHMAN_IP" in os.environ or pytest.config.getoption("is_psyduck", default=False)

class DockerException(Exception):
    """ Exception raised by ``DockerFactory`` for invalid usage """
    pass

class DockerFactory(object):
    """ Factory for interacting with Docker installations """

    DockerException = DockerException

    username = 'memsql'
    password = 'h0r0sh0'

    def __init__(self, request, ports_to_open=None):
        """ Create a machine backed by Docker

        :param request: pytest request fixture to register cleanup of Docker containers
            on exit.
        :param ports_to_open: A list of ports that we will expose on this
           Docker container. By default, we will open 22, and 3306. This
           is only necessary when testing locally; we open all ports by
           default on Psyduck.
        """

        # The below variables are all set by _setup_container().
        self.id = None
        self.host = None
        self.docker_container_host = None
        self.docker_container_port_mappings = {}
        self._setup_container(ports_to_open)
        self._shell = None

        request.addfinalizer(self._stop_container)

        # wait for ssh to accept connections
        time.sleep(5)

    def _get_docker_client(self):
        assert 'DOCKER_HOST' in os.environ, 'Must define DOCKER_HOST, eg: 127.0.0.1:4243'
        import docker

        client = docker.Client(**docker.utils.kwargs_from_env(assert_hostname=False))

        try:
            client.info()
        except Exception as e:
            raise DockerException("Unable to connect to docker, maybe you forgot to define $DOCKER_HOST?\n%s" % e)

        return client

    # XXX
    def _setup_container(self, ports_to_open):
        if is_psyduck():
            assert "PSYDUCK_RUN_ID" in os.environ, "Psyduck run id not found"

            resp = requests.post("http://" + SWITCHMAN_IP + "/api/v1/container-start", data=json.dumps({
                "image": "psy3.memcompute.com/psy3-test-%s:latest" % os.environ['PSYDUCK_RUN_ID'],
                "command": "/bin/sh -c 'while true; do sleep 1; done'"
            }))

            data = resp.json()
            assert 'error' not in data, "Switchman request failure: %s" % resp.content

            self.id = data['id']
            self.host = data['ip']
        else:
            docker_client = self._get_docker_client()

            # rebuild images
            local_shell = LocalShell()

            print("Building %s" % IMAGE_NAME)
            try:
                build_script = os.path.join(REAL_ROOT_PATH, "infra", "build_dockertest.sh")
                command = [ build_script ]
                ret = local_shell.run(command, allow_error=True)
                if ret.return_code != 0:
                    combined_output = ret.output + ret.stderr_output
                    raise DockerException("Failed to build dockertest: %s" % combined_output)
            except NoSuchCommandError:
                raise DockerException("Failed to build dockertest: no build script at %s" % build_script)

            ports_to_open_set = { 22, 3306 }
            if ports_to_open is not None:
                ports_to_open_set.update(ports_to_open)

            port_mapping = {}
            for port in ports_to_open_set:
                port_str = "%d/tcp" % port
                port_mapping[port_str] = {}

            container = docker_client.create_container(
                image=IMAGE_NAME,
                command="/bin/sh -c 'while true; do sleep 1; done'",
                detach=True,
                ports=port_mapping)

            docker_client.start(container=container, publish_all_ports=True, privileged=True)

            inspected = docker_client.inspect_container(container)

            network_settings = inspected["NetworkSettings"]

            is_boot2docker = False
            try:
                ret = local_shell.run([ 'boot2docker', 'ip' ], allow_error=True)
                if ret.return_code == 0:
                    # If we're on Mac OS X, we need to connect through the
                    # boot2docker IP address, not through the host.
                    # Thus, we must use the boot2docker ip command to find the
                    # correct IP address.
                    boot2docker_ip = ret.output.strip()
                    is_boot2docker = True
                    self.docker_container_host = boot2docker_ip
            except NoSuchCommandError:
                pass

            # For locally-hosted Docker containers, we have to make sure that
            # we use the ports that the Docker daemon has assigned when we
            # connect from the test runner to the Docker container (e.g.
            # with docker.shell()).
            if is_boot2docker:
                for port in ports_to_open_set:
                    port_str = "%d/tcp" % port
                    if network_settings["Ports"] and port_str in network_settings["Ports"]:
                        mapped_port = int(network_settings["Ports"][port_str][0]["HostPort"])
                        self.docker_container_port_mappings[port] = mapped_port

            self.id = inspected["Id"]
            self.host = network_settings["IPAddress"]

    @contextmanager
    def shell(self):
        """ SSH into a container and run commands

        Example::

            with docker.shell() as shell:
                print(shell.run(['ls', '/']).output)
        """

        # allow nested contexts
        if self._shell is not None:
            yield self._shell
            return

        host = self.docker_container_host or self.host
        if 22 in self.docker_container_port_mappings:
            ssh_port = self.docker_container_port_mappings[22]
        else:
            ssh_port = 22

        with SSHShell(
                hostname=host,
                port=ssh_port,
                username=self.username,
                password=self.password,
                look_for_private_keys=False) as shell:
            self._shell = shell
            yield shell

        self._shell = None

    def _stop_container(self):
        if is_psyduck():
            resp = requests.post("http://" + SWITCHMAN_IP + "/api/v1/container-stop", data=json.dumps({
                "id": self.id
            }))

            data = resp.json()
            assert 'error' not in data, "Switchman request failure: %s" % resp.content
        else:
            docker_client = self._get_docker_client()
            docker_client.stop(container=self.id)

@pytest.fixture(scope='function')
def docker_factory(request):
    def _factory(*args, **kwargs):
        kwargs['request'] = request
        return DockerFactory(*args, **kwargs)

    return _factory

class LocalContext:
    ROOT_PATH = ROOT_PATH
    MEMSQL_JAR_PATH = MEMSQL_JAR_PATH

    def __init__(self):
        self._shell = LocalShell()
        # NOTE: we need to connect to the spark master using the correct ip
        # so we read it from /etc/hosts which reflects the eth0 interface
        self.external_ip = self._shell.run(["hostname", "-i"]).output

    def deploy_spark(self):
        with open(os.path.join(ROOT_PATH, "memsql-spark/build_receipt.json")) as f:
            data = json.loads(f.read())
            spark_version = data['version']

        self._shell.sudo(["tar", "czvf", "/tmp/memsql-spark.tar.gz", "-C", os.path.join(ROOT_PATH, "memsql-spark"), "."], None)
        self._shell.sudo(["memsql-ops", "file-add", "-t", "spark", "/tmp/memsql-spark.tar.gz"], None)
        return self._shell.run(["memsql-ops", "spark-deploy", "--version", spark_version])

    def kill_spark_interface(self):
        return self._shell.sudo(["pkill", "-f", "com.memsql.spark.interface.Main"], None)

    def deploy_memsql_cluster(self, num_aggs, num_leaves, port = 3306):
        print "Deploying MemSQL master to %d" % port
        self._shell.run(["memsql-ops", "memsql-deploy", "--community-edition", "--role", "master", "--port", str(port)])
        for i in range(num_leaves):
            port += 1
            print "Deploying MemSQL leaf to %d" % port
            self._shell.run(["memsql-ops", "memsql-deploy", "--community-edition", "--role", "leaf", "--port", str(port)])
        for i in range(num_aggs):
            port += 1
            print "Deploying MemSQL child agg to %d" % port
            self._shell.run(["memsql-ops", "memsql-deploy", "--community-edition", "--role", "aggregator", "--port", str(port)])

    def kill_memsql_leaf(self):
        print "Killing MemSQL leaf"
        output = self._shell.run(["memsql-ops", "memsql-list", "--memsql-role", "leaf", "-q"]).output
        self._shell.run(["memsql-ops", "memsql-stop", output.split()[0]])
        self._shell.run(["memsql-ops", "memsql-stop", output.split()[1]])

    def run_ops(self):
        print "Running MemSQL Ops"
        return self._shell.sudo(["memsql-ops", "start"], None)

    def stop_ops(self):
        print "Stopping MemSQL Ops"
        return self._shell.sudo(["memsql-ops", "stop"], None)

    def run_kafka(self, broker_id=0, port=9092):
        print "Running zookeeper"
        self._shell.sudo(["/storage/testroot/zookeeper/bin/zkServer.sh", "start"], DockerFactory.password)
        print "Running kafka"
        return self._shell.spawn(["/storage/testroot/kafka/start.sh"], update_env={
            "EXPOSED_HOST": self.external_ip,
            "EXPOSED_PORT": str(port),
            "ZOOKEEPER_IP": self.external_ip,
            "BROKER_ID": str(broker_id)
        })

    def create_kafka_topic(self, topic, num_partitions=1):
        return self._shell.run([
            "/storage/testroot/kafka/bin/kafka-topics.sh",
            "--create", "--topic", topic,
            "--zookeeper", "%s:2181" % self.external_ip,
            "--partitions", str(num_partitions),
            "--replication-factor", "1"], update_env={

        })

    def get_kafka_topic(self, topic, port=9092, timeout=30):
        start = time.time()
        last_exception = None
        while time.time() < start + timeout:
            try:
                kc = KafkaClient("%s:%d" % (self.external_ip, port))
                return kc.topics[topic]
            except KafkaException as e:
                last_exception = e
        else:
            message = "timed out after %s seconds connecting to kafka" % timeout
            if last_exception is not None:
                message = "%s: %s" % (message, str(last_exception))
            assert False, message

    def spark_submit(self, className, jar=MEMSQL_JAR_PATH, extra_args=[]):
        cmd = [
            os.path.join(SPARK_ROOT, "bin/spark-submit"),
            "--master", "spark://%s:10001" % self.external_ip,
            "--class", className,
            "--deploy-mode", "client",
            jar]

        return self._shell.run(cmd + extra_args, allow_error=True)

    def pipeline_get(self, pipeline_id):
        return requests.get(
            "http://%s:10009/pipeline/get" % self.external_ip,
            headers={ "Content-Type": "application/json" },
            params={
                "pipeline_id": pipeline_id,
            }
        )

    def pipeline_put(self, pipeline_id, config, batch_interval):
        return requests.post(
            "http://%s:10009/pipeline/put" % self.external_ip,
            headers={ "Content-Type": "application/json" },
            params={
                "pipeline_id": pipeline_id,
                "batch_interval": batch_interval
            },
            data=json.dumps(config)
        )

    def pipeline_wait_for_batches(self, pipeline_id, count, timeout=60, since_timestamp=None):
        start = time.time()
        last_exception = None
        while time.time() < start + timeout:
            try:
                resp = requests.get(
                    "http://%s:10009/pipeline/metrics" % self.external_ip,
                    headers={ "Content-Type": "application/json" },
                    params={
                        "pipeline_id": pipeline_id
                    }
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if since_timestamp is not None:
                        # timestamp from metrics is in millseconds
                        batches = filter(lambda x: x['timestamp']/1000 >= since_timestamp and x['event_type'] == 'BatchEnd', data)
                    else:
                        batches = data

                    if len(batches) >= count:
                        return batches[-1]
            except requests.exceptions.RequestException as e:
                # retry until timeout
                last_exception = e
        else:
            message = "waiting for pipeline %s to produce %d batches timed out after %s seconds" % (pipeline_id, count, timeout)
            if last_exception is not None:
                message = "%s: %s" % (message, str(last_exception))
            assert False, message

    def pipeline_update(self, pipeline_id, active, config=None):
        kwargs = {}
        if config is not None:
            kwargs['data'] = json.dumps(config)

        return requests.patch(
            "http://%s:10009/pipeline/update" % self.external_ip,
            headers={ "Content-Type": "application/json" },
            params={
                "pipeline_id": pipeline_id,
                "active": active
            },
            **kwargs
        )

@pytest.fixture(scope='function')
def local_context(request):
    def _factory(*args, **kwargs):
        return LocalContext(*args, **kwargs)

    return _factory
