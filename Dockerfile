FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG TARGETARCH
WORKDIR /src

# Copy csproj and restore as distinct layers
COPY libs/client/*.csproj libs/client/
COPY libs/cluster/*.csproj libs/cluster/
COPY libs/common/*.csproj libs/common/
COPY libs/host/*.csproj libs/host/
COPY libs/server/*.csproj libs/server/
COPY libs/storage/Tsavorite/cs/src/core/*.csproj libs/storage/Tsavorite/cs/src/core/
COPY libs/storage/Tsavorite/cs/src/devices/AzureStorageDevice/*.csproj libs/storage/Tsavorite/cs/src/devices/AzureStorageDevice/
COPY main/GarnetServer/*.csproj main/GarnetServer/
COPY metrics/HdrHistogram/*.csproj metrics/HdrHistogram/
COPY Directory.Build.props Directory.Build.props
COPY Directory.Packages.props Directory.Packages.props

RUN dotnet restore main/GarnetServer/GarnetServer.csproj -a $TARGETARCH

# Copy everthing else and publish app
COPY Garnet.snk Garnet.snk
COPY libs/ libs/
COPY main/ main/
COPY metrics/ metrics/
COPY test/testcerts test/testcerts

WORKDIR /src/main/GarnetServer
RUN dotnet publish -a $TARGETARCH -c Release -o /app --no-restore --self-contained false -f net8.0

# Final stage/image
FROM mcr.microsoft.com/dotnet/runtime:8.0 AS runtime
WORKDIR /app
COPY --from=build /app .

# Run container as a non-root user
USER $APP_UID

# For inter-container communication.
EXPOSE 6379

# Run GarnetServer with an index size of 128MB
ENTRYPOINT ["./GarnetServer", "-i", "128m", "--port", "6379"]
