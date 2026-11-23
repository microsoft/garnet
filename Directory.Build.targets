<Project>
  <PropertyGroup Condition="'$(EnableGarnetAsserts)' == 'true' And '$(MSBuildProjectExtension)' == '.csproj'">
    <DefineConstants>$(DefineConstants);GARNET_ASSERT</DefineConstants>
  </PropertyGroup>
  <ItemGroup Condition="'$(MSBuildProjectExtension)' == '.csproj'">
    <Using Include="Garnet.Diagnostics" />
  </ItemGroup>
  <ItemGroup Condition="'$(MSBuildProjectName)' != 'Garnet.Diagnostics' And '$(MSBuildProjectExtension)' == '.csproj'">
    <ProjectReference Include="$(MSBuildThisFileDirectory)libs/diagnostics/Garnet.Diagnostics.csproj" PrivateAssets="all" />
  </ItemGroup>
</Project>
