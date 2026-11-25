#!/usr/bin/env bash
# Install the boot-time self-heal: a systemd oneshot that runs recover_stack.sh as your user after Docker and
# Tailscale start. Run once, with sudo, from your normal account:
#     sudo scripts/ops/install_recover_service.sh [--unload-ollama]
# Undo: sudo systemctl disable --now atl-transit-recover && sudo rm /etc/systemd/system/atl-transit-recover.service
# Logs: journalctl -u atl-transit-recover -b
set -euo pipefail

[[ $EUID -eq 0 ]] || { echo "run with sudo" >&2; exit 1; }
RUN_AS="${SUDO_USER:?run with sudo from your normal account, not as root}"
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
UNLOAD=0; [[ "${1:-}" == --unload-ollama ]] && UNLOAD=1
id -nG "$RUN_AS" | grep -qw docker || { echo "$RUN_AS must be in the docker group" >&2; exit 1; }

UNIT=/etc/systemd/system/atl-transit-recover.service
cat > "$UNIT" <<EOF
[Unit]
Description=ATL transit stack: clean staged restart after boot
Wants=network-online.target
After=docker.service tailscaled.service network-online.target
Requires=docker.service

[Service]
Type=oneshot
User=$RUN_AS
WorkingDirectory=$REPO_DIR
Environment=ATL_UNLOAD_OLLAMA=$UNLOAD
ExecStart=$REPO_DIR/scripts/ops/recover_stack.sh
TimeoutStartSec=20min

[Install]
WantedBy=multi-user.target
EOF

chmod +x "$REPO_DIR/scripts/ops/recover_stack.sh"
systemctl daemon-reload
systemctl enable atl-transit-recover.service
echo "Installed $UNIT (runs as $RUN_AS, unload Ollama: $UNLOAD). It runs at every boot."
echo "Test now without rebooting: sudo systemctl start atl-transit-recover && journalctl -u atl-transit-recover -b"
