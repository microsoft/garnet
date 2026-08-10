ARG DOTNET_TAG=10.0

FROM mcr.microsoft.com/dotnet/sdk:${DOTNET_TAG} AS build
WORKDIR /src

COPY Directory.Build.props Directory.Packages.props Version.props Garnet.snk ./
COPY libs/storage/Tsavorite/cs/src/core/ libs/storage/Tsavorite/cs/src/core/
COPY playground/LightEpochLitmus/ playground/LightEpochLitmus/

RUN dotnet publish playground/LightEpochLitmus/LightEpochLitmus.csproj -f net10.0 -c Release -o /app

FROM mcr.microsoft.com/dotnet/runtime:${DOTNET_TAG}
WORKDIR /app
COPY --from=build /app .

ENTRYPOINT ["dotnet", "/app/LightEpochLitmus.dll"]
