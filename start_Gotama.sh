#!/bin/bash
rm data/Gotama/GOTAMA.mdf /data/Gotama/GOTAMA.ldf
podman run -d --pod sdlb --hostname mssqlserver --add-host mssqlserver:127.0.0.1 --name mssql -v ${PWD}/data:/data  -v ${PWD}/config:/config -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=%abcd1234%" mcr.microsoft.com/mssql/server:2017-latest
sleep 10  ### wait until the MSSQL server is ready (maybe a very conservative time)
podman exec -it mssql /opt/mssql-tools/bin/sqlcmd -S mssqlserver -U sa -P '%abcd1234%' -Q "RESTORE DATABASE GotamaMaster FROM DISK = '/data/Gotama/20220406_SQL16_MASTER_GOTAMA.BAK' WITH REPLACE, MOVE 'GotamaV0_Data' TO '/data/Gotama/GOTAMA.mdf', MOVE 'GotamaV0_Log' TO '/data/Gotama/GOTAMA.ldf'"
