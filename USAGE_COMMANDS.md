# Slurm Job Watcher 使用命令

进入工具目录：

```bash
cd /data/home/intern001/zhoutianyu/slurm_job_watcher
```

## 1. 测试邮件发送

```bash
python3 slurm_job_watcher.py test-mail
```

看到下面输出表示 SMTP 邮件可用：

```text
Test email sent.
```

## 2. 提交新任务并自动加入监控

推荐用这个方式提交训练任务：

```bash
python3 slurm_job_watcher.py submit /path/to/train.sbatch --name exp001
```

示例：

```bash
python3 slurm_job_watcher.py submit /data/home/intern001/zhoutianyu/project/train.sbatch --name qwen_lora_exp001
```

如果需要给 `sbatch` 追加参数：

```bash
python3 slurm_job_watcher.py submit /path/to/train.sbatch --name exp001 -- --partition regular
```

## 3. 监控已经提交的任务

如果你已经手动提交了任务：

```bash
sbatch train.sbatch
```

假设返回：

```text
Submitted batch job 142900
```

把这个任务加入监控：

```bash
python3 slurm_job_watcher.py add 142900 --name exp001
```

## 4. 启动监控器

前台运行：

```bash
python3 slurm_job_watcher.py run
```

后台运行：

```bash
nohup ./watch.sh > watcher.log 2>&1 &
```

后台运行后查看日志：

```bash
tail -f watcher.log
```

## 5. 查看正在监控的任务

```bash
python3 slurm_job_watcher.py list
```

## 6. 单次检查任务状态

```bash
python3 slurm_job_watcher.py run --once
```

## 7. 从监控列表移除任务

```bash
python3 slurm_job_watcher.py remove <job_id>
```

示例：

```bash
python3 slurm_job_watcher.py remove 142900
```

## 8. 查看 Slurm 任务状态

```bash
squeue -j <job_id>
```

```bash
sacct -j <job_id> --format=JobID,JobName,State,ExitCode,Start,End,Elapsed
```

## 9. 停止后台监控器

查找进程：

```bash
ps -ef | grep slurm_job_watcher.py | grep -v grep
```

停止进程：

```bash
kill <pid>
```

## 10. 常用完整流程

提交任务并开始后台监控：

```bash
cd /data/home/intern001/zhoutianyu/slurm_job_watcher
python3 slurm_job_watcher.py submit /path/to/train.sbatch --name exp001
nohup ./watch.sh > watcher.log 2>&1 &
tail -f watcher.log
```

监控已有任务：

```bash
cd /data/home/intern001/zhoutianyu/slurm_job_watcher
python3 slurm_job_watcher.py add <job_id> --name exp001
nohup ./watch.sh > watcher.log 2>&1 &
tail -f watcher.log
```

## 11. 和 openpi 轮询提交脚本一起使用

现在下面这个脚本已经集成邮件监控：

```bash
/data/home/intern001/zhoutianyu/openpi/train_scripts/poll_submit_vehicle_physical_button_press.py
```

照常运行即可。它在成功 `sbatch` 后会自动：

- 把 Slurm job id 加入 `slurm_job_watcher/jobs.json`
- 启动后台邮件监控进程
- 在任务开始、完成、失败、取消、超时、OOM 等状态变化时发送邮件

常用命令：

```bash
/data/home/intern001/zhoutianyu/openpi/train_scripts/poll_submit_vehicle_physical_button_press.py \
  --job-script /data/home/intern001/zhoutianyu/openpi/train_scripts/train_vehicle_physical_button_press.sh \
  --request-gpus 4 \
  --request-cpus 24 \
  --poll-interval 15 \
  --start-timeout 3600 \
  --state-check-interval 10 \
  --max-polls 0 \
  --gpu-nodes gpuh2001,gpuh2002
```

如果只想提交，不启用邮件监控：

```bash
/data/home/intern001/zhoutianyu/openpi/train_scripts/poll_submit_vehicle_physical_button_press.py --no-mail-watch
```

如果只注册任务，但不自动启动后台监控器：

```bash
/data/home/intern001/zhoutianyu/openpi/train_scripts/poll_submit_vehicle_physical_button_press.py --no-start-watcher
```

查看轮询脚本自动启动的 watcher 日志：

```bash
tail -f /data/home/intern001/zhoutianyu/slurm_job_watcher/watcher.log
```

## 邮件触发时机

任务状态变化时会发送邮件，例如：

- `PENDING -> RUNNING`
- `RUNNING -> COMPLETED`
- `RUNNING -> FAILED`
- `RUNNING -> CANCELLED`
- `RUNNING -> TIMEOUT`
- `RUNNING -> OUT_OF_MEMORY`
- `RUNNING -> NODE_FAIL`

任务进入完成、失败、取消、超时、OOM 等终止状态后，会自动标记为 inactive，不再继续重复通知。
