## 1st Change to be done
Check Current Mode:-
```bash
cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
```

Change Current Mode to performance:-
```bash
echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
```
Note: This resets after a reboot


## 2nd Change to be done(Permanent)
1. Stop the current Ollama server:
```bash
sudo systemctl stop ollama
```

2. sudo systemctl edit ollama.service
```bash
sudo systemctl edit ollama.service
```

3. In the editor that opens, add these lines in the empty space between the comments:
```Ini, TOML
[Service]
Environment="OLLAMA_FLASH_ATTENTION=1"
```

4. Save and exit (Ctrl+X, then Y, then Enter), then restart Ollama:
```bash
sudo systemctl daemon-reload
sudo systemctl start ollama
```
