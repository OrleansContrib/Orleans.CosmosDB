@echo OFF
set containerName=azure-cosmosdb-emulator
set hostDirectory=%LOCALAPPDATA%\azure-cosmosdb-emulator.hostd
md %hostDirectory% 2>nul
docker run --name %containerName% --memory 2GB --mount "type=bind,source=%hostDirectory%,destination=C:\CosmosDB.Emulator\bind-mount" -P --interactive --tty microsoft/azure-cosmosdb-emulator