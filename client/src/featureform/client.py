import warnings
from .register import ResourceClient
from .serving import ServingClient
from .enums import ApplicationMode
import pandas as pd


class Client(ResourceClient, ServingClient):
    def __init__(
        self, host=None, local=False, insecure=False, cert_path=None, dry_run=False
    ):
        ResourceClient.__init__(
            self,
            host=host,
            local=local,
            insecure=insecure,
            cert_path=cert_path,
            dry_run=dry_run,
        )
        if not dry_run:
            ServingClient.__init__(
                self, host=host, local=local, insecure=insecure, cert_path=cert_path
            )
            self.application_mode = (
                ApplicationMode.LOCAL if local else ApplicationMode.HOSTED
            )

    def compute_df(self, name, variant="default"):
        if self.application_mode == ApplicationMode.LOCAL:
            return self.impl.get_input_df(name, variant)
        elif self.application_mode == ApplicationMode.HOSTED:
            warnings.warn(
                "Computing dataframes on sources in hosted mode is not yet supported."
            )
            return pd.DataFrame()
        else:
            raise ValueError(f"ApplicationMode {ApplicationMode} not supported.")
