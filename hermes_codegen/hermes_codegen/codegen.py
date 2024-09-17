import sys, os
from hermes_codegen.util.paths import HERMES_ROOT

class HermesCodegen:
    def make_configs(self):
        self.create_config(
            path=f"{HERMES_ROOT}/config/hermes_client_default.yaml",
            var_name="kHermesClientDefaultConfigStr",
            config_path=f"{HERMES_ROOT}/include/hermes/config_client_default.h",
            macro_name="HERMES_CLIENT"
        )

        self.create_config(
            path=f"{HERMES_ROOT}/config/hermes_server_default.yaml",
            var_name="kHermesServerDefaultConfigStr",
            config_path=f"{HERMES_ROOT}/include/hermes/config_server_default.h",
            macro_name="HERMES_SERVER"
        )

    def create_config(self, path, var_name, config_path, macro_name):
        with open(path) as fp:
            yaml_config_lines = fp.read().splitlines()

        # Create the hermes config string
        string_lines = []
        string_lines.append(f"const inline char* {var_name} = ")
        for line in yaml_config_lines:
            line = line.replace('\"', '\\\"')
            line = line.replace('\'', '\\\'')
            string_lines.append(f"\"{line}\\n\"")
        string_lines[-1] = string_lines[-1] + ';'

        # Create the configuration
        config_lines = []
        config_lines.append(f"#ifndef HRUN_SRC_CONFIG_{macro_name}_DEFAULT_H_")
        config_lines.append(f"#define HRUN_SRC_CONFIG_{macro_name}_DEFAULT_H_")
        config_lines += string_lines
        config_lines.append(f"#endif  // HRUN_SRC_CONFIG_{macro_name}_DEFAULT_H_")

        # Persist
        config = "\n".join(config_lines)
        with open(config_path, 'w') as fp:
            fp.write(config)
