# Beacon deployment

Ansible role + playbook that configures a beacon node. The playbook handles everything from OS packages upwards: nginx, TLS via Let's Encrypt (HTTP-01), the beacon systemd unit, and the beacon binary itself.

## Prerequisites

- Ansible 2.16+ (`pipx install ansible-core`)
- Each beacon host reachable on SSH as `ubuntu` with the matching `.pem` key at the path listed in your inventory
- DNS A record for each beacon hostname pointing at the host
- Ports 22/80/443/TCP and 4172/UDP open in the host's security group
- Beacon binary built: `make build-beacon` from the repo root

## Configure inventory

Copy `inventory.example.yml` to your own inventory file (kept outside this public repo, e.g., in a private operator repo or alongside your secrets) and fill in real values:

```yaml
your-beacon-1.example.com:
  pop_id: eu-west-2          # arbitrary stable ID
  geo: London                # display label
  ansible_ssh_private_key_file: ~/.ssh/your_key.pem
```

## Deploy all beacons

```bash
make build-beacon
cd deploy/ansible
ansible-playbook -i /path/to/your/inventory.yml playbook.yml
```

## Target a single host

```bash
ansible-playbook -i /path/to/your/inventory.yml playbook.yml --limit your-beacon-1.example.com
```

## Dry-run

```bash
ansible-playbook -i /path/to/your/inventory.yml playbook.yml --check --diff
```

## Adding a new POP

1. Provision the box (public subnet, public IP, SG with 22/80/443/TCP + 4172/UDP).
2. Add an A record at your DNS provider pointing the hostname at the box.
3. Add a host block to your inventory with `pop_id`, `geo`, and `ansible_ssh_private_key_file`.
4. Run the playbook with `--limit <host>`.
5. Register the new POP with your ingest service so the desktop client picks it up.
