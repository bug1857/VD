#!/bin/zsh

set -eu

if (( $# != 4 )); then
  print -u2 'usage: capture_resource_snapshot.sh TYPE OUTPUT_PATH RUN_DIR COLLECTION_PREFIX'
  exit 64
fi

vd_snapshot_type=$1
vd_snapshot_output=$2
vd_snapshot_run_dir=$3
vd_snapshot_collection_prefix=$4
vd_snapshot_ps_file=$(mktemp -t vd-exp001-snapshot-ps.XXXXXX)
trap 'rm -f "$vd_snapshot_ps_file"' EXIT

ps -axo pid=,ppid=,%cpu=,%mem=,state=,etime=,comm=,args= > "$vd_snapshot_ps_file"

{
  printf 'snapshot_script_version=2\n'
  printf 'snapshot_script_sha256=%s\n' "$(shasum -a 256 "$0" | awk '{print $1}')"
  printf 'snapshot_type=%s\n' "$vd_snapshot_type"
  printf 'timestamp_utc=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  printf 'timestamp_local=%s\n' "$(date '+%Y-%m-%dT%H:%M:%S%z')"
  printf 'run_dir=%s\n' "$vd_snapshot_run_dir"
  printf 'collection_prefix=%s\n' "$vd_snapshot_collection_prefix"
  printf 'git_commit=%s\n' "$(git rev-parse HEAD)"
  printf 'git_status_begin\n'
  git -c core.fsmonitor=false status --porcelain=v1
  printf 'git_status_end\n'
  uptime
  pmset -g therm
  pmset -g batt
  vm_stat
  memory_pressure
  df -h /

  printf 'named_application_check_begin\n'
  awk '
    /\/Applications\/Google Chrome\.app\// ||
    /\/Safari\.app\/Contents\/MacOS\/Safari( |$)/ ||
    /\/Applications\/CopyClip\.app\// ||
    /\/Applications\/Amphetamine\.app\// ||
    /\/Applications\/Raycast\.app\// ||
    /\/Applications\/Rocket\.app\// ||
    /\/Applications\/AlDente[^ ]*\.app\// {
      print
      count++
    }
    END { printf "named_application_match_count=%d\n", count + 0 }
  ' "$vd_snapshot_ps_file"
  printf 'named_application_check_end\n'

  printf 'other_application_check_begin\n'
  awk '
    index($0, " /Applications/") &&
    !index($0, "/Applications/Docker.app/") &&
    !index($0, "/Applications/ChatGPT.app/") {
      print
      count++
    }
    END { printf "other_application_match_count=%d\n", count + 0 }
  ' "$vd_snapshot_ps_file"
  printf 'other_application_check_end\n'

  printf 'nonessential_codex_check_begin\n'
  awk '
    /\/Codex Computer Use\.app\// ||
    /\/SkyComputerUseService( |$)/ ||
    /\/node_repl( |$)/ ||
    /--utility-sub-type=on_device_model\.mojom\.OnDeviceModelService/ {
      print
      count++
    }
    END { printf "nonessential_codex_match_count=%d\n", count + 0 }
  ' "$vd_snapshot_ps_file"
  printf 'nonessential_codex_check_end\n'

  printf 'required_codex_runtime_check_begin\n'
  awk '
    /\/Applications\/ChatGPT\.app\// &&
    (/--utility-sub-type=audio\.mojom\.AudioService/ ||
     /--utility-sub-type=video_capture\.mojom\.VideoCaptureService/) {
      print
      count++
    }
    END { printf "required_codex_runtime_match_count=%d\n", count + 0 }
  ' "$vd_snapshot_ps_file"
  printf 'required_codex_runtime_check_end\n'

  printf 'process_snapshot_begin\n'
  ps -axo pid=,ppid=,%cpu=,%mem=,state=,etime=,comm= | sort -k3 -nr
  printf 'process_snapshot_end\n'
  printf 'top_snapshot_begin\n'
  top -l 1 -o cpu -n 80 -stats pid,command,cpu,mem,threads,state
  printf 'top_snapshot_end\n'
  printf 'docker_info='; docker info --format 'CPUs={{.NCPU}} Memory={{.MemTotal}}'
  printf 'docker_compose_ps_begin\n'
  docker compose --env-file infra/milvus/env-001/env001.env -p env-001 -f infra/milvus/env-001/compose.vendor.yml -f infra/milvus/env-001/compose.override.yml ps 2>/dev/null
  printf 'docker_compose_ps_end\n'
  printf 'docker_stats_begin\n'
  docker stats --no-stream --format '{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.NetIO}}\t{{.BlockIO}}\t{{.PIDs}}'
  printf 'docker_stats_end\n'
  printf 'milvus_health='; curl -fsS http://localhost:9091/healthz; printf '\n'
  printf 'snapshot_complete=true\n'
} > "$vd_snapshot_output" 2>&1
