"""Install the Pulumi plugin of every installed provider package, at image build time.

The Python provider packages do not contain the provider binary, which `pulumi up` would download at run time. Each
carries a `pulumi-plugin.json` (name, version, server); this installs those plugins into PULUMI_HOME so the Jobs
need no download. Run once in the Dockerfile: `python install_plugins.py`.
"""

import json
import subprocess
from importlib.metadata import distributions


def main() -> None:
    plugins = set()
    for dist in distributions():
        for file in dist.files or []:
            if file.name == "pulumi-plugin.json":
                info = json.loads(file.read_text())
                if info.get("resource"):
                    plugins.add((info["name"], info["version"], info.get("server") or ""))
    for name, version, server in sorted(plugins):
        command = ["pulumi", "plugin", "install", "resource", name, version.removeprefix("v")]
        subprocess.run([*command, *(["--server", server] if server else [])], check=True)  # noqa: S603


if __name__ == "__main__":
    main()
