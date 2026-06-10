# Slurm Job Watcher

This tool watches Slurm jobs with `squeue` and `sacct`, then sends email through your own SMTP account when job state changes.

It does not depend on Slurm's broken `--mail-type` setup.

## 1. Initialize

```bash
cd /data/home/intern001/zhoutianyu/slurm_job_watcher
python3 slurm_job_watcher.py init
```

Create a local config:

```bash
cp config.example.ini config.ini
```

Edit `config.ini`:

```ini
[smtp]
host = smtp.qq.com
port = 465
use_ssl = true
username = your_email@qq.com
from = your_email@qq.com
to = receive_email@example.com
password_env = SLURM_WATCHER_SMTP_PASSWORD
```

Set your SMTP authorization code in the environment:

```bash
export SLURM_WATCHER_SMTP_PASSWORD='your_smtp_authorization_code'
```

For QQ, 163, Gmail, and Outlook, this is usually an SMTP authorization code or app password, not your normal login password.

## 2. Test SMTP

```bash
python3 slurm_job_watcher.py test-mail
```

If this succeeds, the watcher can send emails without Slurm's mail system.

## 3. Add an Existing Job

```bash
python3 slurm_job_watcher.py add 142900 --name exp001
```

## 4. Submit and Watch a New Job

```bash
python3 slurm_job_watcher.py submit /path/to/train.sbatch --name exp001
```

Extra sbatch arguments can be appended:

```bash
python3 slurm_job_watcher.py submit /path/to/train.sbatch --name exp001 -- --partition regular
```

## 5. Run Watcher

Foreground:

```bash
python3 slurm_job_watcher.py run
```

Background:

```bash
nohup python3 slurm_job_watcher.py run > watcher.log 2>&1 &
```

Check once:

```bash
python3 slurm_job_watcher.py run --once
```

List watched jobs:

```bash
python3 slurm_job_watcher.py list
```

Remove a job:

```bash
python3 slurm_job_watcher.py remove 142900
```

## Events

The watcher sends email when a job state changes, including:

- `PENDING -> RUNNING`
- `RUNNING -> COMPLETED`
- `RUNNING -> FAILED`
- `RUNNING -> CANCELLED`
- `RUNNING -> TIMEOUT`
- `RUNNING -> OUT_OF_MEMORY`
- `RUNNING -> NODE_FAIL`

Terminal jobs are automatically marked inactive.
