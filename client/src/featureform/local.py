import featureform as ff
import argparse
local = ff.register_local_directory(
  path="...",
)
local.register_file("iris.csv")