"""
This module provides classes and methods to launch the HermesRun service.
hrun is ....
"""

from jarvis_cd.basic.pkg import Service, Color
from jarvis_util import *


class HermesRun(Service):
    """
    This class provides methods to launch the HermesRun service.
    """
    def _init(self):
        """
        Initialize paths
        """
        self.daemon_pkg = None
        self.hostfile_path = f'{self.shared_dir}/hostfile'
        pass

    def _configure_menu(self):
        """
        Create a CLI menu for the configurator method.
        For thorough documentation of these parameters, view:
        https://github.com/scs-lab/jarvis-util/wiki/3.-Argument-Parsing

        :return: List(dict)
        """
        return [

            {
                'name': 'recency_max',
                'msg': 'time before blob is considered stale (sec)',
                'type': float,
                'default': 1,
                'class': 'buffer organizer',
                'rank': 1,
            },
            {
                'name': 'borg_min_cap',
                'msg': 'Capacity percentage before reorganizing can begin',
                'type': float,
                'default': 0,
                'class': 'buffer organizer',
                'rank': 1,
            },
            {
                'name': 'flush_period',
                'msg': 'Period of time to check for flushing (milliseconds)',
                'type': int,
                'default': 5000,
                'class': 'buffer organizer',
                'rank': 1,
            },
            {
                'name': 'include',
                'msg': 'Specify paths to include',
                'type': list,
                'default': [],
                'class': 'adapter',
                'rank': 1,
                'args': [
                    {
                        'name': 'path',
                        'msg': 'The path to be included',
                        'type': str
                    },
                ],
                'aliases': ['i']
            },
            {
                'name': 'exclude',
                'msg': 'Specify paths to exclude',
                'type': list,
                'default': [],
                'class': 'adapter',
                'rank': 1,
                'args': [
                    {
                        'name': 'path',
                        'msg': 'The path to be excluded',
                        'type': str
                    },
                ],
                'aliases': ['e']
            },
            {
                'name': 'adapter_mode',
                'msg': 'The adapter mode to use for Hermes',
                'type': str,
                'default': 'default',
                'choices': ['default', 'scratch', 'bypass'],
                'class': 'adapter',
                'rank': 1,
            },
            {
                'name': 'flush_mode',
                'msg': 'The flushing mode to use for adapters',
                'type': str,
                'default': 'async',
                'choices': ['sync', 'async'],
                'class': 'adapter',
                'rank': 1,
            },
            {
                'name': 'log_verbosity',
                'msg': 'Verbosity of the output, 0 for fatal, 1 for info',
                'type': int,
                'default': '1',
            },
            {
                'name': 'page_size',
                'msg': 'The page size to use for adapters',
                'type': str,
                'default': '1m',
                'class': 'adapter',
                'rank': 1,
            },
            {
                'name': 'ram',
                'msg': 'Amount of RAM to use for buffering',
                'type': str,
                'default': '0',
                'class': 'dpe',
                'rank': 1,
            },
            {
                'name': 'dpe',
                'msg': 'The DPE to use by default',
                'type': str,
                'default': 'MinimizeIoTime',
                'class': 'dpe',
                'rank': 1,
            },
            {
                'name': 'devices',
                'msg': 'Search for a number of devices to include',
                'type': list,
                'default': [],
                'args': [
                    {
                        'name': 'mount',
                        'msg': 'The path to the device',
                        'type': str
                    },
                    {
                        'name': 'size',
                        'msg': 'The amount of data to use',
                        'type': str
                    }
                ],
                'class': 'dpe',
                'rank': 1,
            }
        ]
    
    def _configure(self, **kwargs):
        """
        Converts the Jarvis configuration to application-specific configuration.
        E.g., OrangeFS produces an orangefs.xml file.

        :param config: The human-readable jarvis YAML configuration for the
        application.
        :return: None
        """
        rg = self.jarvis.resource_graph
        hermes_server = {
            'devices': {},
        }

        # Create hostfile
        self.hostfile = self.jarvis.hostfile
        self.env['HERMES_LOG_VERBOSITY'] = str(self.config['log_verbosity'])

        # Begin making Hermes client config
        hermes_client = {
            'path_inclusions': [''],
            'path_exclusions': ['/'],
            'file_page_size': self.config['page_size']
        }
        if self.config['flush_mode'] == 'async':
            hermes_client['flushing_mode'] = 'kAsync'
        elif self.config['flush_mode'] == 'sync':
            hermes_client['flushing_mode'] = 'kSync'
        if self.config['include'] is not None:
            hermes_client['path_inclusions'] += self.config['include']
        if self.config['exclude'] is not None:
            hermes_client['path_exclusions'] += self.config['exclude']

        # Get storage info
        devs = []
        if len(self.config['devices']) == 0:
            # Get all the fastest storage device mount points on machine
            dev_df = rg.find_storage(needs_root=False)
            devs = dev_df.rows
        else:
            # Get the storage devices for the user 
            for dev in self.config['devices']:
                devs.append({
                    'mount': dev['mount'],
                    'avail': int(SizeConv.to_int(dev['size'])  * 1 / .9),
                    'shared': False,
                    'dev_type': 'custom'
                })
        if len(devs) == 0:
            raise Exception('Hermes needs at least one storage device')
        self.config['borg_paths'] = []
        for i, dev in enumerate(devs):
            dev_type = dev['dev_type']
            custom_name = f'{dev_type}_{i}'
            mount = os.path.expandvars(dev['mount'])
            if len(mount) == 0:
                continue
            mount = f'{mount}/hermes_data'
            hermes_server['devices'][custom_name] = {
                'mount_point': f'fs://{mount}',
                'capacity': int(.9 * float(dev['avail'])),
                'block_size': '4kb',
                'is_shared_device': dev['shared'],
                'borg_capacity_thresh': [0.0, 1.0],
                'slab_sizes': ['4KB', '16KB', '64KB', '1MB']
            }
            self.config['borg_paths'].append(mount)
            Mkdir(mount, PsshExecInfo(hostfile=self.hostfile,
                                      env=self.env))
        if 'ram' in self.config and self.config['ram'] != '0':
            hermes_server['devices']['ram'] = {
                'mount_point': 'ram://',
                'capacity': self.config['ram'],
                'block_size': '4kb',
                'is_shared_device': False,
                'borg_capacity_thresh': [self.config['borg_min_cap'], 1.0],
                'slab_sizes': ['256', '512', '1KB',
                               '4KB', '16KB', '64KB', '1MB']
            }

        # Get network Info
        hermes_server['buffer_organizer'] = {
            'recency_max': self.config['recency_max'],
            'flush_period': self.config['flush_period']
        }
        hermes_server['default_placement_policy'] = self.config['dpe']
        if self.config['adapter_mode'] == 'default':
            adapter_mode = 'kDefault'
        elif self.config['adapter_mode'] == 'scratch':
            adapter_mode = 'kScratch'
        elif self.config['adapter_mode'] == 'bypass':
            adapter_mode = 'kBypass'
        self.env['HERMES_ADAPTER_MODE'] = adapter_mode
        hermes_server['default_placement_policy'] = self.config['dpe']

        # Save hermes configurations
        hermes_server_yaml = f'{self.shared_dir}/hermes_server.yaml'
        YamlFile(hermes_server_yaml).save(hermes_server)
        self.env['HERMES_CONF'] = hermes_server_yaml

        # Save Hermes client configurations
        hermes_client_yaml = f'{self.shared_dir}/hermes_client.yaml'
        YamlFile(hermes_client_yaml).save(hermes_client)
        self.env['HERMES_CLIENT_CONF'] = hermes_client_yaml

    def start(self):
        """
        Launch an application. E.g., OrangeFS will launch the servers, clients,
        and metadata services on all necessary pkgs.

        :return: None
        """
        self.log(self.env['HERMES_CONF'])
        self.log(self.env['HERMES_CLIENT_CONF'])
        pass

    def stop(self):
        """
        Stop a running application. E.g., OrangeFS will terminate the servers,
        clients, and metadata services.

        :return: None
        """
        pass

    def kill(self):
       pass

    def clean(self):
        """
        Destroy all data for an application. E.g., OrangeFS will delete all
        metadata and data directories in addition to the orangefs.xml file.

        :return: None
        """
        self.hostfile = self.jarvis.hostfile
        for path in self.config['borg_paths']:
            self.log(f'Removing {path}', Color.YELLOW)
            Rm(path, PsshExecInfo(hostfile=self.hostfile))

    def status(self):
        """
        Check whether or not an application is running. E.g., are OrangeFS
        servers running?

        :return: True or false
        """
        return True
