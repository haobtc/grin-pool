import argparse
import logging
import json
import pathlib
import paramiko
import tarfile
import time
import threading

from pudb import set_trace


paramiko.util.log_to_file("simple-backup-wallet.log")
CONFIG_JSON = pathlib.Path(__file__).parent / 'config.json'

"""
config.json
{
    "wallet_directory": "~/.grin/floo/wallet_data",
    "hosts": [
        "remote1",
        "remote2",
        "remote3",
    ],
    "backup_directory": "/usr/local/var/.grin/backup/",
}
"""


class Grin:
    def __init__(self, config=None, path=None):
        if config is None and path is None:
            config = Grin.from_config()
        elif config is None:
            config = Grin.from_config(path)
        self.initialize(config)

    @staticmethod
    def from_config(path=CONFIG_JSON):
        try:
            fp = open(path, 'r')
            obj = json.load(fp)
            print(obj)
            return obj
        except OSError as e:
            logging.error(e)
            raise
        except json.JSONDecodeError as e:
            logging.error(e)
            raise

    def initialize(self, config):
        self.wallet_directory = config['wallet_directory']
        self.hosts = config['hosts']
        self.backup_directory = config['backup_directory']


class SSHClient:
    def __init__(self, host, username,
                 port=22, password=None, key_filename=None):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.key_filename = key_filename
        self.build_client()

    def build_client(self, timeout=10):
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy)
        if self.password:
            client.connect(
                    self.host,
                    self.port,
                    self.username,
                    password=self.password,
                    timeout=timeout)
        elif self.key_filename:
            client.connect(
                    self.host,
                    self.port,
                    self.username,
                    key_filename=self.key_filename,
                    timeout=timeout)
        else:
            raise ValueError("Should have either password or id_rsa pub key")

        # use sftp client can implement all operations like scp
        self.client = paramiko.SFTPClient.from_transport(client.get_transport())

    def put(self, src, dst, callback=None, confirm=True):
        src = pathlib.Path(src)
        dst = pathlib.Path(dst)

        filename = src.name
        if not filename.endswith(".tar"):
            filename = "{}-{}.tar".format(src.name, int(time.time()))
            print(filename)
            with tarfile.open(src / filename, 'w') as tarf:
                tarf.add(src, arcname=src.name)

        try:
            resp = self.client.put(str(src / filename), str(dst / filename), callback, confirm) 
        except Exception as e:
            logging.error(e)


def run(args):
    set_trace()
    grin = Grin(path=args.config)
    threads = []
    for grin.host in grin.hosts:
        client = SSHClient(grin.host, args.username,
                           password=args.password, key_filename=args.key)
        t = threading.Thread(target=client.put,
                             args=(grin.wallet_directory, grin.backup_directory))
        threads.append(t)

    logging.info("Begin Backup wallet data")
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Grin config file location.')
    parser.add_argument('-u', '--username', help='Remote server username, default to all hosts for now.')
    parser.add_argument('-p', '--password', help='Remote server password, default to all hosts for now.')
    parser.add_argument('-k', '--key', help='Default to ssh id_rsa pub key')

    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()
