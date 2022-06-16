import featureform as ff

local = ff.register_local_directory(
  path="...",
)
local.register_file("iris.csv")