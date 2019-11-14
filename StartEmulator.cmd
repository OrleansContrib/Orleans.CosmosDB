@echo OFF
set containerName=azure-cosmosdb-emulator
set hostDirectory=%LOCALAPPDATA%\azure-cosmosdb-emulator.hostd
md %hostDirectory% 2>nul
REM docker run --name %containerName% --memory 2GB --mount "type=bind,source=%hostDirectory%,destination=C:\CosmosDB.Emulator\bind-mount" -P --interactive --tty microsoft/azure-cosmosdb-emulator
docker run --name %containerName% --memory 2GB --mount "type=bind,source=%hostDirectory%,destination=C:\CosmosDB.Emulator\bind-mount" -p 8081:8081 -p 8900:8900 -p 8901:8901 -p 8979:8979 -p 10250:10250 -p 10251:10251 -p 10252:10252 -p 10253:10253 -p 10254:10254 -p 10255:10255 -p 10256:10256 -p 10350:10350 microsoft/azure-cosmosdb-emulator
