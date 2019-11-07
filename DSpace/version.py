from pbr import version as pbr_version

T2STOR_VENDOR = "T2STOR"
T2STOR_PRODUCT = "T2STOR Portal"
T2STOR_PACKAGE = None  # OS distro package version suffix

loaded = False
version_info = pbr_version.VersionInfo('Athena')
version_string = version_info.version_string
