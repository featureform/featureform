import featureform as ff

client = ff.Client("localhost:7878", insecure=True)
ff.register_gcs(
    name="test",
    bucket_name="test",
    credentials=ff.GCPCredentials(
        project_id="test",
        credentials_path="/Users/sdreyer/Projects/embeddinghub/tests/end_to_end/test_files/dummy_creds.json",
    ),
    path="/",
)
client.apply()
# p = client.get_provider("test")
# print(p)
