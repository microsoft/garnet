FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:9.0 AS build
ARG TARGETARCH
WORKDIR /src

# Copy csproj and restore as distinct layers
COPY libs/client/*.csproj libs/client/
COPY libs/cluster/*.csproj libs/cluster/
COPY libs/common/*.csproj libs/common/
COPY libs/host/*.csproj libs/host/
COPY libs/server/*.csproj libs/server/
COPY libs/resources/*.csproj libs/resources/
COPY libs/storage/Tsavorite/cs/src/core/*.csproj libs/storage/Tsavorite/cs/src/core/
COPY libs/storage/Tsavorite/cs/src/devices/AzureStorageDevice/*.csproj libs/storage/Tsavorite/cs/src/devices/AzureStorageDevice/
COPY main/GarnetServer/*.csproj main/GarnetServer/
COPY metrics/HdrHistogram/*.csproj metrics/HdrHistogram/
COPY Directory.Build.props Directory.Build.props
COPY Directory.Packages.props Directory.Packages.props
COPY Version.props Version.props
COPY .editorconfig .editorconfig

RUN dotnet restore main/GarnetServer/GarnetServer.csproj -a $TARGETARCH

# Copy everthing else and publish app
COPY Garnet.snk Garnet.snk
COPY libs/ libs/
COPY main/ main/
COPY metrics/ metrics/
COPY test/testcerts test/testcerts

# Set protected mode to no for Docker images
RUN sed -i 's/"ProtectedMode": "yes",/"ProtectedMode": "no",/' /src/libs/host/defaults.conf

WORKDIR /src/main/GarnetServer
RUN dotnet publish -a $TARGETARCH -c Release -o /app --no-restore --self-contained false -f net8.0 -p:EnableSourceLink=false -p:EnableSourceControlManagerQueries=false

# Delete xmldoc files
RUN find /app -name '*.xml' -delete

# Final stage/image
FROM mcr.microsoft.com/dotnet/runtime:8.0 AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       libaio1 \
       liblua5.4-0 \
       dpkg-dev \
    && ARCH="$(dpkg-architecture -qDEB_HOST_MULTIARCH)" \
    && DN_DIR=$(ls -d /usr/share/dotnet/shared/Microsoft.NETCore.App/* 2>/dev/null | head -n1 || true) \
    && if [ -n "$DN_DIR" ]; then ln -sf "/usr/lib/${ARCH}/liblua5.4.so.0" "$DN_DIR/liblua54.so"; fi \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir /data /app \
    && chown -R $APP_UID:$APP_UID /data /app

VOLUME /data

WORKDIR /app
COPY --from=build --chown=$APP_UID:$APP_UID /app /app

# Run container as a non-root user
USER $APP_UID

# For inter-container communication.
EXPOSE 6379

ENTRYPOINT ["/app/GarnetServer"]
