# How to sync BGP Updates/Ribs and Validate

## 1. Sync ROAs & BGP Updates/Ribs

```
python3 0_get_roa.py 2021-05-01 2021-05-02
python3 0_get_udpates.py 2021-05-01-00:00:00 2021-05-02-00:00:00 -r
python3 0_get_udpates.py 2021-05-01-00:00:00 2021-05-02-00:00:00 -u
```

## 2. Process Updates/Ribs
```
python3 1_process_update.py 2021-05-01-00:00:00 2021-05-02-00:00:00 -r
python3 1_process_update.py 2021-05-01-00:00:00 2021-05-02-00:00:00 -u
```

## 3. Add AS_PATH to the result
```
python3 2_add_path.py 2021-05-01-00:00:00 2021-05-02-00:00:00 add_path.log -r
python3 2_add_path.py 2021-05-01-00:00:00 2021-05-02-00:00:00 add_path.log -u
```
