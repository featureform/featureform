import json
import os
import shutil
import stat

import featureform
from flask.testing import FlaskClient
import pytest
from featureform.cli import app

features = [
    {
        "all-variants": ["quickstart"],
        "type": "Feature",
        "default-variant": "quickstart",
        "name": "avg_transactions",
        "variants": {
            "quickstart": {
                "description": "",
                "entity": "user",
                "name": "avg_transactions",
                "owner": "default_user",
                "provider": "local-mode",
                "data-type": "float32",
                "variant": "quickstart",
                "status": "ready",
                "location": {
                    "entity": "CustomerID",
                    "value": "TransactionAmount",
                    "timestamp": "",
                },
                "source": {"Name": "average_user_transaction", "Variant": "quickstart"},
                "tags": [],
                "properties": {},
            }
        },
    }
]
labels = [
    {
        "all-variants": ["quickstart"],
        "type": "Label",
        "default-variant": "quickstart",
        "name": "fraudulent",
        "variants": {
            "quickstart": {
                "description": "",
                "entity": "user",
                "data-type": "bool",
                "name": "fraudulent",
                "owner": "default_user",
                "provider": "",
                "variant": "quickstart",
                "status": "ready",
                "location": {
                    "entity": "CustomerID",
                    "value": "IsFraud",
                    "timestamp": "",
                },
                "source": {"Name": "transactions", "Variant": "quickstart"},
                "training-sets": {
                    "fraud_training": [
                        {
                            "description": "",
                            "name": "fraud_training",
                            "owner": "default_user",
                            "variant": "quickstart",
                            "status": "ready",
                            "label": {"Name": "fraudulent", "Variant": "quickstart"},
                            "features": {
                                "avg_transactions": [
                                    {
                                        "description": "",
                                        "entity": "user",
                                        "name": "avg_transactions",
                                        "owner": "default_user",
                                        "provider": "local-mode",
                                        "data-type": "float32",
                                        "variant": "quickstart",
                                        "status": "ready",
                                        "location": {
                                            "entity": "CustomerID",
                                            "value": "TransactionAmount",
                                            "timestamp": "",
                                        },
                                        "source": {
                                            "Name": "average_user_transaction",
                                            "Variant": "quickstart",
                                        },
                                        "tags": [],
                                        "properties": {},
                                    }
                                ]
                            },
                            "tags": [],
                            "properties": {},
                        }
                    ]
                },
                "tags": [],
                "properties": {},
            }
        },
    }
]
sources = [
    {
        "all-variants": ["quickstart"],
        "type": "Source",
        "default-variant": "quickstart",
        "name": "transactions",
        "variants": {
            "quickstart": {
                "description": "A dataset of fraudulent transactions",
                "name": "transactions",
                "source-type": "Source",
                "owner": "default_user",
                "provider": "local-mode",
                "variant": "quickstart",
                "status": "ready",
                "labels": {
                    "fraudulent": [
                        {
                            "description": "",
                            "entity": "user",
                            "data-type": "bool",
                            "name": "fraudulent",
                            "owner": "default_user",
                            "provider": "",
                            "variant": "quickstart",
                            "status": "ready",
                            "location": {
                                "entity": "CustomerID",
                                "value": "IsFraud",
                                "timestamp": "",
                            },
                            "source": {"Name": "transactions", "Variant": "quickstart"},
                            "training-sets": {
                                "fraud_training": [
                                    {
                                        "description": "",
                                        "name": "fraud_training",
                                        "owner": "default_user",
                                        "variant": "quickstart",
                                        "status": "ready",
                                        "label": {
                                            "Name": "fraudulent",
                                            "Variant": "quickstart",
                                        },
                                        "features": {
                                            "avg_transactions": [
                                                {
                                                    "description": "",
                                                    "entity": "user",
                                                    "name": "avg_transactions",
                                                    "owner": "default_user",
                                                    "provider": "local-mode",
                                                    "data-type": "float32",
                                                    "variant": "quickstart",
                                                    "status": "ready",
                                                    "location": {
                                                        "entity": "CustomerID",
                                                        "value": "TransactionAmount",
                                                        "timestamp": "",
                                                    },
                                                    "source": {
                                                        "Name": "average_user_transaction",
                                                        "Variant": "quickstart",
                                                    },
                                                    "tags": [],
                                                    "properties": {},
                                                }
                                            ]
                                        },
                                        "tags": [],
                                        "properties": {},
                                    }
                                ]
                            },
                            "tags": [],
                            "properties": {},
                        }
                    ]
                },
                "features": {},
                "training-sets": {
                    "fraud_training": [
                        {
                            "description": "",
                            "name": "fraud_training",
                            "owner": "default_user",
                            "variant": "quickstart",
                            "status": "ready",
                            "label": {"Name": "fraudulent", "Variant": "quickstart"},
                            "features": {
                                "avg_transactions": [
                                    {
                                        "description": "",
                                        "entity": "user",
                                        "name": "avg_transactions",
                                        "owner": "default_user",
                                        "provider": "local-mode",
                                        "data-type": "float32",
                                        "variant": "quickstart",
                                        "status": "ready",
                                        "location": {
                                            "entity": "CustomerID",
                                            "value": "TransactionAmount",
                                            "timestamp": "",
                                        },
                                        "source": {
                                            "Name": "average_user_transaction",
                                            "Variant": "quickstart",
                                        },
                                        "tags": [],
                                        "properties": {},
                                    }
                                ]
                            },
                            "tags": [],
                            "properties": {},
                        }
                    ]
                },
                "tags": [],
                "properties": {},
            }
        },
    },
    {
        "all-variants": ["quickstart"],
        "type": "Source",
        "default-variant": "quickstart",
        "name": "average_user_transaction",
        "variants": {
            "quickstart": {
                "description": "the average transaction amount for a user",
                "name": "average_user_transaction",
                "source-type": "Source",
                "owner": "default_user",
                "provider": "local-mode",
                "variant": "quickstart",
                "status": "ready",
                "labels": {},
                "features": {
                    "avg_transactions": [
                        {
                            "description": "",
                            "entity": "user",
                            "name": "avg_transactions",
                            "owner": "default_user",
                            "provider": "local-mode",
                            "data-type": "float32",
                            "variant": "quickstart",
                            "status": "ready",
                            "location": {
                                "entity": "CustomerID",
                                "value": "TransactionAmount",
                                "timestamp": "",
                            },
                            "source": {
                                "Name": "average_user_transaction",
                                "Variant": "quickstart",
                            },
                            "tags": [],
                            "properties": {},
                        }
                    ]
                },
                "training-sets": {
                    "fraud_training": [
                        {
                            "description": "",
                            "name": "fraud_training",
                            "owner": "default_user",
                            "variant": "quickstart",
                            "status": "ready",
                            "label": {"Name": "fraudulent", "Variant": "quickstart"},
                            "features": {
                                "avg_transactions": [
                                    {
                                        "description": "",
                                        "entity": "user",
                                        "name": "avg_transactions",
                                        "owner": "default_user",
                                        "provider": "local-mode",
                                        "data-type": "float32",
                                        "variant": "quickstart",
                                        "status": "ready",
                                        "location": {
                                            "entity": "CustomerID",
                                            "value": "TransactionAmount",
                                            "timestamp": "",
                                        },
                                        "source": {
                                            "Name": "average_user_transaction",
                                            "Variant": "quickstart",
                                        },
                                        "tags": [],
                                        "properties": {},
                                    }
                                ]
                            },
                            "tags": [],
                            "properties": {},
                        }
                    ]
                },
                "tags": [],
                "properties": {},
            }
        },
    },
]
training_sets = [
    {
        "all-variants": ["quickstart"],
        "type": "TrainingSet",
        "default-variant": "quickstart",
        "name": "fraud_training",
        "variants": {
            "quickstart": {
                "description": "",
                "name": "fraud_training",
                "owner": "default_user",
                "variant": "quickstart",
                "status": "ready",
                "label": {"Name": "fraudulent", "Variant": "quickstart"},
                "features": {
                    "avg_transactions": [
                        {
                            "description": "",
                            "entity": "user",
                            "name": "avg_transactions",
                            "owner": "default_user",
                            "provider": "local-mode",
                            "data-type": "float32",
                            "variant": "quickstart",
                            "status": "ready",
                            "location": {
                                "entity": "CustomerID",
                                "value": "TransactionAmount",
                                "timestamp": "",
                            },
                            "source": {
                                "Name": "average_user_transaction",
                                "Variant": "quickstart",
                            },
                            "tags": [],
                            "properties": {},
                        }
                    ]
                },
                "tags": [],
                "properties": {},
            }
        },
    }
]
entities = [
    {
        "description": "",
        "type": "Entity",
        "name": "user",
        "features": {
            "avg_transactions": [
                {
                    "description": "",
                    "entity": "user",
                    "name": "avg_transactions",
                    "owner": "default_user",
                    "provider": "local-mode",
                    "data-type": "float32",
                    "variant": "quickstart",
                    "status": "ready",
                    "location": {
                        "entity": "CustomerID",
                        "value": "TransactionAmount",
                        "timestamp": "",
                    },
                    "source": {
                        "Name": "average_user_transaction",
                        "Variant": "quickstart",
                    },
                    "tags": [],
                    "properties": {},
                }
            ]
        },
        "labels": {
            "fraudulent": [
                {
                    "description": "",
                    "entity": "user",
                    "data-type": "bool",
                    "name": "fraudulent",
                    "owner": "default_user",
                    "provider": "",
                    "variant": "quickstart",
                    "status": "ready",
                    "location": {
                        "entity": "CustomerID",
                        "value": "IsFraud",
                        "timestamp": "",
                    },
                    "source": {"Name": "transactions", "Variant": "quickstart"},
                    "training-sets": {
                        "fraud_training": [
                            {
                                "description": "",
                                "name": "fraud_training",
                                "owner": "default_user",
                                "variant": "quickstart",
                                "status": "ready",
                                "label": {
                                    "Name": "fraudulent",
                                    "Variant": "quickstart",
                                },
                                "features": {
                                    "avg_transactions": [
                                        {
                                            "description": "",
                                            "entity": "user",
                                            "name": "avg_transactions",
                                            "owner": "default_user",
                                            "provider": "local-mode",
                                            "data-type": "float32",
                                            "variant": "quickstart",
                                            "status": "ready",
                                            "location": {
                                                "entity": "CustomerID",
                                                "value": "TransactionAmount",
                                                "timestamp": "",
                                            },
                                            "source": {
                                                "Name": "average_user_transaction",
                                                "Variant": "quickstart",
                                            },
                                            "tags": [],
                                            "properties": {},
                                        }
                                    ]
                                },
                                "tags": [],
                                "properties": {},
                            }
                        ]
                    },
                    "tags": [],
                    "properties": {},
                }
            ]
        },
        "training-sets": {
            "fraud_training": [
                {
                    "description": "",
                    "name": "fraud_training",
                    "owner": "default_user",
                    "variant": "quickstart",
                    "status": "ready",
                    "label": {"Name": "fraudulent", "Variant": "quickstart"},
                    "features": {
                        "avg_transactions": [
                            {
                                "description": "",
                                "entity": "user",
                                "name": "avg_transactions",
                                "owner": "default_user",
                                "provider": "local-mode",
                                "data-type": "float32",
                                "variant": "quickstart",
                                "status": "ready",
                                "location": {
                                    "entity": "CustomerID",
                                    "value": "TransactionAmount",
                                    "timestamp": "",
                                },
                                "source": {
                                    "Name": "average_user_transaction",
                                    "Variant": "quickstart",
                                },
                                "tags": [],
                                "properties": {},
                            }
                        ]
                    },
                    "tags": [],
                    "properties": {},
                }
            ]
        },
        "status": "ready",
        "tags": [],
        "properties": {},
    }
]
models = []
users = [
    {
        "name": "default_user",
        "type": "User",
        "features": {
            "avg_transactions": [
                {
                    "description": "",
                    "entity": "user",
                    "name": "avg_transactions",
                    "owner": "default_user",
                    "provider": "local-mode",
                    "data-type": "float32",
                    "variant": "quickstart",
                    "status": "ready",
                    "location": {
                        "entity": "CustomerID",
                        "value": "TransactionAmount",
                        "timestamp": "",
                    },
                    "source": {
                        "Name": "average_user_transaction",
                        "Variant": "quickstart",
                    },
                    "tags": [],
                    "properties": {},
                }
            ]
        },
        "labels": {
            "fraudulent": [
                {
                    "description": "",
                    "entity": "user",
                    "data-type": "bool",
                    "name": "fraudulent",
                    "owner": "default_user",
                    "provider": "",
                    "variant": "quickstart",
                    "status": "ready",
                    "location": {
                        "entity": "CustomerID",
                        "value": "IsFraud",
                        "timestamp": "",
                    },
                    "source": {"Name": "transactions", "Variant": "quickstart"},
                    "training-sets": {
                        "fraud_training": [
                            {
                                "description": "",
                                "name": "fraud_training",
                                "owner": "default_user",
                                "variant": "quickstart",
                                "status": "ready",
                                "label": {
                                    "Name": "fraudulent",
                                    "Variant": "quickstart",
                                },
                                "features": {
                                    "avg_transactions": [
                                        {
                                            "description": "",
                                            "entity": "user",
                                            "name": "avg_transactions",
                                            "owner": "default_user",
                                            "provider": "local-mode",
                                            "data-type": "float32",
                                            "variant": "quickstart",
                                            "status": "ready",
                                            "location": {
                                                "entity": "CustomerID",
                                                "value": "TransactionAmount",
                                                "timestamp": "",
                                            },
                                            "source": {
                                                "Name": "average_user_transaction",
                                                "Variant": "quickstart",
                                            },
                                            "tags": [],
                                            "properties": {},
                                        }
                                    ]
                                },
                                "tags": [],
                                "properties": {},
                            }
                        ]
                    },
                    "tags": [],
                    "properties": {},
                }
            ]
        },
        "training-sets": {
            "fraud_training": [
                {
                    "description": "",
                    "name": "fraud_training",
                    "owner": "default_user",
                    "variant": "quickstart",
                    "status": "ready",
                    "label": {"Name": "fraudulent", "Variant": "quickstart"},
                    "features": {
                        "avg_transactions": [
                            {
                                "description": "",
                                "entity": "user",
                                "name": "avg_transactions",
                                "owner": "default_user",
                                "provider": "local-mode",
                                "data-type": "float32",
                                "variant": "quickstart",
                                "status": "ready",
                                "location": {
                                    "entity": "CustomerID",
                                    "value": "TransactionAmount",
                                    "timestamp": "",
                                },
                                "source": {
                                    "Name": "average_user_transaction",
                                    "Variant": "quickstart",
                                },
                                "tags": [],
                                "properties": {},
                            }
                        ]
                    },
                    "tags": [],
                    "properties": {},
                }
            ]
        },
        "sources": {
            "transactions": [
                {
                    "description": "A dataset of fraudulent transactions",
                    "name": "transactions",
                    "source-type": "Source",
                    "owner": "default_user",
                    "provider": "local-mode",
                    "variant": "quickstart",
                    "status": "ready",
                    "labels": {
                        "fraudulent": [
                            {
                                "description": "",
                                "entity": "user",
                                "data-type": "bool",
                                "name": "fraudulent",
                                "owner": "default_user",
                                "provider": "",
                                "variant": "quickstart",
                                "status": "ready",
                                "location": {
                                    "entity": "CustomerID",
                                    "value": "IsFraud",
                                    "timestamp": "",
                                },
                                "source": {
                                    "Name": "transactions",
                                    "Variant": "quickstart",
                                },
                                "training-sets": {
                                    "fraud_training": [
                                        {
                                            "description": "",
                                            "name": "fraud_training",
                                            "owner": "default_user",
                                            "variant": "quickstart",
                                            "status": "ready",
                                            "label": {
                                                "Name": "fraudulent",
                                                "Variant": "quickstart",
                                            },
                                            "features": {
                                                "avg_transactions": [
                                                    {
                                                        "description": "",
                                                        "entity": "user",
                                                        "name": "avg_transactions",
                                                        "owner": "default_user",
                                                        "provider": "local-mode",
                                                        "data-type": "float32",
                                                        "variant": "quickstart",
                                                        "status": "ready",
                                                        "location": {
                                                            "entity": "CustomerID",
                                                            "value": "TransactionAmount",
                                                            "timestamp": "",
                                                        },
                                                        "source": {
                                                            "Name": "average_user_transaction",
                                                            "Variant": "quickstart",
                                                        },
                                                    }
                                                ]
                                            },
                                        }
                                    ]
                                },
                            }
                        ]
                    },
                    "features": {},
                    "training-sets": {
                        "fraud_training": [
                            {
                                "description": "",
                                "name": "fraud_training",
                                "owner": "default_user",
                                "variant": "quickstart",
                                "status": "ready",
                                "label": {
                                    "Name": "fraudulent",
                                    "Variant": "quickstart",
                                },
                                "features": {
                                    "avg_transactions": [
                                        {
                                            "description": "",
                                            "entity": "user",
                                            "name": "avg_transactions",
                                            "owner": "default_user",
                                            "provider": "local-mode",
                                            "data-type": "float32",
                                            "variant": "quickstart",
                                            "status": "ready",
                                            "location": {
                                                "entity": "CustomerID",
                                                "value": "TransactionAmount",
                                                "timestamp": "",
                                            },
                                            "source": {
                                                "Name": "average_user_transaction",
                                                "Variant": "quickstart",
                                            },
                                        }
                                    ]
                                },
                            }
                        ]
                    },
                }
            ],
            "average_user_transaction": [
                {
                    "description": "the average transaction amount for a user",
                    "name": "average_user_transaction",
                    "source-type": "Source",
                    "owner": "default_user",
                    "provider": "local-mode",
                    "variant": "quickstart",
                    "status": "ready",
                    "labels": {},
                    "features": {
                        "avg_transactions": [
                            {
                                "description": "",
                                "entity": "user",
                                "name": "avg_transactions",
                                "owner": "default_user",
                                "provider": "local-mode",
                                "data-type": "float32",
                                "variant": "quickstart",
                                "status": "ready",
                                "location": {
                                    "entity": "CustomerID",
                                    "value": "TransactionAmount",
                                    "timestamp": "",
                                },
                                "source": {
                                    "Name": "average_user_transaction",
                                    "Variant": "quickstart",
                                },
                            }
                        ]
                    },
                    "training-sets": {
                        "fraud_training": [
                            {
                                "description": "",
                                "name": "fraud_training",
                                "owner": "default_user",
                                "variant": "quickstart",
                                "status": "ready",
                                "label": {
                                    "Name": "fraudulent",
                                    "Variant": "quickstart",
                                },
                                "features": {
                                    "avg_transactions": [
                                        {
                                            "description": "",
                                            "entity": "user",
                                            "name": "avg_transactions",
                                            "owner": "default_user",
                                            "provider": "local-mode",
                                            "data-type": "float32",
                                            "variant": "quickstart",
                                            "status": "ready",
                                            "location": {
                                                "entity": "CustomerID",
                                                "value": "TransactionAmount",
                                                "timestamp": "",
                                            },
                                            "source": {
                                                "Name": "average_user_transaction",
                                                "Variant": "quickstart",
                                            },
                                        }
                                    ]
                                },
                            }
                        ]
                    },
                }
            ],
        },
        "status": "ready",
        "tags": [],
        "properties": {},
    }
]
providers = [
    {
        "name": "local-mode",
        "type": "Provider",
        "description": "This is local mode",
        "provider-type": "LOCAL_ONLINE",
        "software": "localmode",
        "team": "team",
        "sources": {
            "transactions": [
                {
                    "description": "A dataset of fraudulent transactions",
                    "name": "transactions",
                    "source-type": "Source",
                    "owner": "default_user",
                    "provider": "local-mode",
                    "variant": "quickstart",
                    "status": "ready",
                    "labels": {
                        "fraudulent": [
                            {
                                "description": "",
                                "entity": "user",
                                "data-type": "bool",
                                "name": "fraudulent",
                                "owner": "default_user",
                                "provider": "",
                                "variant": "quickstart",
                                "status": "ready",
                                "location": {
                                    "entity": "CustomerID",
                                    "value": "IsFraud",
                                    "timestamp": "",
                                },
                                "source": {
                                    "Name": "transactions",
                                    "Variant": "quickstart",
                                },
                                "training-sets": {
                                    "fraud_training": [
                                        {
                                            "description": "",
                                            "name": "fraud_training",
                                            "owner": "default_user",
                                            "variant": "quickstart",
                                            "status": "ready",
                                            "label": {
                                                "Name": "fraudulent",
                                                "Variant": "quickstart",
                                            },
                                            "features": {
                                                "avg_transactions": [
                                                    {
                                                        "description": "",
                                                        "entity": "user",
                                                        "name": "avg_transactions",
                                                        "owner": "default_user",
                                                        "provider": "local-mode",
                                                        "data-type": "float32",
                                                        "variant": "quickstart",
                                                        "status": "ready",
                                                        "location": {
                                                            "entity": "CustomerID",
                                                            "value": "TransactionAmount",
                                                            "timestamp": "",
                                                        },
                                                        "source": {
                                                            "Name": "average_user_transaction",
                                                            "Variant": "quickstart",
                                                        },
                                                        "tags": [],
                                                        "properties": {},
                                                    }
                                                ]
                                            },
                                            "tags": [],
                                            "properties": {},
                                        }
                                    ]
                                },
                                "tags": [],
                                "properties": {},
                            }
                        ]
                    },
                    "features": {},
                    "training-sets": {
                        "fraud_training": [
                            {
                                "description": "",
                                "name": "fraud_training",
                                "owner": "default_user",
                                "variant": "quickstart",
                                "status": "ready",
                                "label": {
                                    "Name": "fraudulent",
                                    "Variant": "quickstart",
                                },
                                "features": {
                                    "avg_transactions": [
                                        {
                                            "description": "",
                                            "entity": "user",
                                            "name": "avg_transactions",
                                            "owner": "default_user",
                                            "provider": "local-mode",
                                            "data-type": "float32",
                                            "variant": "quickstart",
                                            "status": "ready",
                                            "location": {
                                                "entity": "CustomerID",
                                                "value": "TransactionAmount",
                                                "timestamp": "",
                                            },
                                            "source": {
                                                "Name": "average_user_transaction",
                                                "Variant": "quickstart",
                                            },
                                            "tags": [],
                                            "properties": {},
                                        }
                                    ]
                                },
                                "tags": [],
                                "properties": {},
                            }
                        ]
                    },
                    "tags": [],
                    "properties": {},
                }
            ],
            "average_user_transaction": [
                {
                    "description": "the average transaction amount for a user",
                    "name": "average_user_transaction",
                    "source-type": "Source",
                    "owner": "default_user",
                    "provider": "local-mode",
                    "variant": "quickstart",
                    "status": "ready",
                    "labels": {},
                    "features": {
                        "avg_transactions": [
                            {
                                "description": "",
                                "entity": "user",
                                "name": "avg_transactions",
                                "owner": "default_user",
                                "provider": "local-mode",
                                "data-type": "float32",
                                "variant": "quickstart",
                                "status": "ready",
                                "location": {
                                    "entity": "CustomerID",
                                    "value": "TransactionAmount",
                                    "timestamp": "",
                                },
                                "source": {
                                    "Name": "average_user_transaction",
                                    "Variant": "quickstart",
                                },
                                "tags": [],
                                "properties": {},
                            }
                        ]
                    },
                    "training-sets": {
                        "fraud_training": [
                            {
                                "description": "",
                                "name": "fraud_training",
                                "owner": "default_user",
                                "variant": "quickstart",
                                "status": "ready",
                                "label": {
                                    "Name": "fraudulent",
                                    "Variant": "quickstart",
                                },
                                "features": {
                                    "avg_transactions": [
                                        {
                                            "description": "",
                                            "entity": "user",
                                            "name": "avg_transactions",
                                            "owner": "default_user",
                                            "provider": "local-mode",
                                            "data-type": "float32",
                                            "variant": "quickstart",
                                            "status": "ready",
                                            "location": {
                                                "entity": "CustomerID",
                                                "value": "TransactionAmount",
                                                "timestamp": "",
                                            },
                                            "source": {
                                                "Name": "average_user_transaction",
                                                "Variant": "quickstart",
                                            },
                                            "tags": [],
                                            "properties": {},
                                        }
                                    ]
                                },
                                "tags": [],
                                "properties": {},
                            }
                        ]
                    },
                    "tags": [],
                    "properties": {},
                }
            ],
        },
        "features": {
            "avg_transactions": [
                {
                    "description": "",
                    "entity": "user",
                    "name": "avg_transactions",
                    "owner": "default_user",
                    "provider": "local-mode",
                    "data-type": "float32",
                    "variant": "quickstart",
                    "status": "ready",
                    "location": {
                        "entity": "CustomerID",
                        "value": "TransactionAmount",
                        "timestamp": "",
                    },
                    "source": {
                        "Name": "average_user_transaction",
                        "Variant": "quickstart",
                    },
                    "tags": [],
                    "properties": {},
                }
            ]
        },
        "labels": {},
        "status": "status",
        "serializedConfig": "{}",
        "tags": ["local-mode"],
        "properties": {"resource_type": "Provider"},
    }
]

default_user = {
    "name": "default_user",
    "type": "User",
    "features": {
        "avg_transactions": [
            {
                "description": "",
                "entity": "user",
                "name": "avg_transactions",
                "owner": "default_user",
                "provider": "local-mode",
                "data-type": "float32",
                "variant": "quickstart",
                "status": "ready",
                "location": {
                    "entity": "CustomerID",
                    "value": "TransactionAmount",
                    "timestamp": "",
                },
                "source": {"Name": "average_user_transaction", "Variant": "quickstart"},
                "tags": [],
                "properties": {},
            }
        ]
    },
    "labels": {
        "fraudulent": [
            {
                "description": "",
                "entity": "user",
                "data-type": "bool",
                "name": "fraudulent",
                "owner": "default_user",
                "provider": "",
                "variant": "quickstart",
                "status": "ready",
                "location": {
                    "entity": "CustomerID",
                    "value": "IsFraud",
                    "timestamp": "",
                },
                "source": {"Name": "transactions", "Variant": "quickstart"},
                "training-sets": {
                    "fraud_training": [
                        {
                            "description": "",
                            "name": "fraud_training",
                            "owner": "default_user",
                            "variant": "quickstart",
                            "status": "ready",
                            "label": {"Name": "fraudulent", "Variant": "quickstart"},
                            "features": {
                                "avg_transactions": [
                                    {
                                        "description": "",
                                        "entity": "user",
                                        "name": "avg_transactions",
                                        "owner": "default_user",
                                        "provider": "local-mode",
                                        "data-type": "float32",
                                        "variant": "quickstart",
                                        "status": "ready",
                                        "location": {
                                            "entity": "CustomerID",
                                            "value": "TransactionAmount",
                                            "timestamp": "",
                                        },
                                        "source": {
                                            "Name": "average_user_transaction",
                                            "Variant": "quickstart",
                                        },
                                        "tags": [],
                                        "properties": {},
                                    }
                                ]
                            },
                            "tags": [],
                            "properties": {},
                        }
                    ]
                },
                "tags": [],
                "properties": {},
            }
        ]
    },
    "training-sets": {
        "fraud_training": [
            {
                "description": "",
                "name": "fraud_training",
                "owner": "default_user",
                "variant": "quickstart",
                "status": "ready",
                "label": {"Name": "fraudulent", "Variant": "quickstart"},
                "features": {
                    "avg_transactions": [
                        {
                            "description": "",
                            "entity": "user",
                            "name": "avg_transactions",
                            "owner": "default_user",
                            "provider": "local-mode",
                            "data-type": "float32",
                            "variant": "quickstart",
                            "status": "ready",
                            "location": {
                                "entity": "CustomerID",
                                "value": "TransactionAmount",
                                "timestamp": "",
                            },
                            "source": {
                                "Name": "average_user_transaction",
                                "Variant": "quickstart",
                            },
                            "tags": [],
                            "properties": {},
                        }
                    ]
                },
                "tags": [],
                "properties": {},
            }
        ]
    },
    "sources": {
        "transactions": [
            {
                "description": "A dataset of fraudulent transactions",
                "name": "transactions",
                "source-type": "Source",
                "owner": "default_user",
                "provider": "local-mode",
                "variant": "quickstart",
                "status": "ready",
                "labels": {
                    "fraudulent": [
                        {
                            "description": "",
                            "entity": "user",
                            "data-type": "bool",
                            "name": "fraudulent",
                            "owner": "default_user",
                            "provider": "",
                            "variant": "quickstart",
                            "status": "ready",
                            "location": {
                                "entity": "CustomerID",
                                "value": "IsFraud",
                                "timestamp": "",
                            },
                            "source": {"Name": "transactions", "Variant": "quickstart"},
                            "training-sets": {
                                "fraud_training": [
                                    {
                                        "description": "",
                                        "name": "fraud_training",
                                        "owner": "default_user",
                                        "variant": "quickstart",
                                        "status": "ready",
                                        "label": {
                                            "Name": "fraudulent",
                                            "Variant": "quickstart",
                                        },
                                        "features": {
                                            "avg_transactions": [
                                                {
                                                    "description": "",
                                                    "entity": "user",
                                                    "name": "avg_transactions",
                                                    "owner": "default_user",
                                                    "provider": "local-mode",
                                                    "data-type": "float32",
                                                    "variant": "quickstart",
                                                    "status": "ready",
                                                    "location": {
                                                        "entity": "CustomerID",
                                                        "value": "TransactionAmount",
                                                        "timestamp": "",
                                                    },
                                                    "source": {
                                                        "Name": "average_user_transaction",
                                                        "Variant": "quickstart",
                                                    },
                                                    "tags": [],
                                                    "properties": {},
                                                }
                                            ]
                                        },
                                        "tags": [],
                                        "properties": {},
                                    }
                                ]
                            },
                            "tags": [],
                            "properties": {},
                        }
                    ]
                },
                "features": {},
                "training-sets": {
                    "fraud_training": [
                        {
                            "description": "",
                            "name": "fraud_training",
                            "owner": "default_user",
                            "variant": "quickstart",
                            "status": "ready",
                            "label": {"Name": "fraudulent", "Variant": "quickstart"},
                            "features": {
                                "avg_transactions": [
                                    {
                                        "description": "",
                                        "entity": "user",
                                        "name": "avg_transactions",
                                        "owner": "default_user",
                                        "provider": "local-mode",
                                        "data-type": "float32",
                                        "variant": "quickstart",
                                        "status": "ready",
                                        "location": {
                                            "entity": "CustomerID",
                                            "value": "TransactionAmount",
                                            "timestamp": "",
                                        },
                                        "source": {
                                            "Name": "average_user_transaction",
                                            "Variant": "quickstart",
                                        },
                                        "tags": [],
                                        "properties": {},
                                    }
                                ]
                            },
                            "tags": [],
                            "properties": {},
                        }
                    ]
                },
                "tags": [],
                "properties": {},
            }
        ],
        "average_user_transaction": [
            {
                "description": "the average transaction amount for a user",
                "name": "average_user_transaction",
                "source-type": "Source",
                "owner": "default_user",
                "provider": "local-mode",
                "variant": "quickstart",
                "status": "ready",
                "labels": {},
                "features": {
                    "avg_transactions": [
                        {
                            "description": "",
                            "entity": "user",
                            "name": "avg_transactions",
                            "owner": "default_user",
                            "provider": "local-mode",
                            "data-type": "float32",
                            "variant": "quickstart",
                            "status": "ready",
                            "location": {
                                "entity": "CustomerID",
                                "value": "TransactionAmount",
                                "timestamp": "",
                            },
                            "source": {
                                "Name": "average_user_transaction",
                                "Variant": "quickstart",
                            },
                            "tags": [],
                            "properties": {},
                        }
                    ]
                },
                "training-sets": {
                    "fraud_training": [
                        {
                            "description": "",
                            "name": "fraud_training",
                            "owner": "default_user",
                            "variant": "quickstart",
                            "status": "ready",
                            "label": {"Name": "fraudulent", "Variant": "quickstart"},
                            "features": {
                                "avg_transactions": [
                                    {
                                        "description": "",
                                        "entity": "user",
                                        "name": "avg_transactions",
                                        "owner": "default_user",
                                        "provider": "local-mode",
                                        "data-type": "float32",
                                        "variant": "quickstart",
                                        "status": "ready",
                                        "location": {
                                            "entity": "CustomerID",
                                            "value": "TransactionAmount",
                                            "timestamp": "",
                                        },
                                        "source": {
                                            "Name": "average_user_transaction",
                                            "Variant": "quickstart",
                                        },
                                        "tags": [],
                                        "properties": {},
                                    }
                                ]
                            },
                            "tags": [],
                            "properties": {},
                        }
                    ]
                },
                "tags": [],
                "properties": {},
            }
        ],
    },
    "status": "ready",
    "tags": [],
    "properties": {},
}
localmode = {
    "name": "local-mode",
    "type": "Provider",
    "description": "This is local mode",
    "provider-type": "LOCAL_ONLINE",
    "software": "localmode",
    "team": "team",
    "sources": {
        "transactions": [
            {
                "description": "A dataset of fraudulent transactions",
                "name": "transactions",
                "source-type": "Source",
                "owner": "default_user",
                "provider": "local-mode",
                "variant": "quickstart",
                "status": "ready",
                "labels": {
                    "fraudulent": [
                        {
                            "description": "",
                            "entity": "user",
                            "data-type": "bool",
                            "name": "fraudulent",
                            "owner": "default_user",
                            "provider": "",
                            "variant": "quickstart",
                            "status": "ready",
                            "location": {
                                "entity": "CustomerID",
                                "value": "IsFraud",
                                "timestamp": "",
                            },
                            "source": {"Name": "transactions", "Variant": "quickstart"},
                            "training-sets": {
                                "fraud_training": [
                                    {
                                        "description": "",
                                        "name": "fraud_training",
                                        "owner": "default_user",
                                        "variant": "quickstart",
                                        "status": "ready",
                                        "label": {
                                            "Name": "fraudulent",
                                            "Variant": "quickstart",
                                        },
                                        "features": {
                                            "avg_transactions": [
                                                {
                                                    "description": "",
                                                    "entity": "user",
                                                    "name": "avg_transactions",
                                                    "owner": "default_user",
                                                    "provider": "local-mode",
                                                    "data-type": "float32",
                                                    "variant": "quickstart",
                                                    "status": "ready",
                                                    "location": {
                                                        "entity": "CustomerID",
                                                        "value": "TransactionAmount",
                                                        "timestamp": "",
                                                    },
                                                    "source": {
                                                        "Name": "average_user_transaction",
                                                        "Variant": "quickstart",
                                                    },
                                                    "tags": [],
                                                    "properties": {},
                                                }
                                            ]
                                        },
                                        "tags": [],
                                        "properties": {},
                                    }
                                ]
                            },
                            "tags": [],
                            "properties": {},
                        }
                    ]
                },
                "features": {},
                "training-sets": {
                    "fraud_training": [
                        {
                            "description": "",
                            "name": "fraud_training",
                            "owner": "default_user",
                            "variant": "quickstart",
                            "status": "ready",
                            "label": {"Name": "fraudulent", "Variant": "quickstart"},
                            "features": {
                                "avg_transactions": [
                                    {
                                        "description": "",
                                        "entity": "user",
                                        "name": "avg_transactions",
                                        "owner": "default_user",
                                        "provider": "local-mode",
                                        "data-type": "float32",
                                        "variant": "quickstart",
                                        "status": "ready",
                                        "location": {
                                            "entity": "CustomerID",
                                            "value": "TransactionAmount",
                                            "timestamp": "",
                                        },
                                        "source": {
                                            "Name": "average_user_transaction",
                                            "Variant": "quickstart",
                                        },
                                        "tags": [],
                                        "properties": {},
                                    }
                                ]
                            },
                            "tags": [],
                            "properties": {},
                        }
                    ]
                },
                "tags": [],
                "properties": {},
            }
        ],
        "average_user_transaction": [
            {
                "description": "the average transaction amount for a user",
                "name": "average_user_transaction",
                "source-type": "Source",
                "owner": "default_user",
                "provider": "local-mode",
                "variant": "quickstart",
                "status": "ready",
                "labels": {},
                "features": {
                    "avg_transactions": [
                        {
                            "description": "",
                            "entity": "user",
                            "name": "avg_transactions",
                            "owner": "default_user",
                            "provider": "local-mode",
                            "data-type": "float32",
                            "variant": "quickstart",
                            "status": "ready",
                            "location": {
                                "entity": "CustomerID",
                                "value": "TransactionAmount",
                                "timestamp": "",
                            },
                            "source": {
                                "Name": "average_user_transaction",
                                "Variant": "quickstart",
                            },
                            "tags": [],
                            "properties": {},
                        }
                    ]
                },
                "training-sets": {
                    "fraud_training": [
                        {
                            "description": "",
                            "name": "fraud_training",
                            "owner": "default_user",
                            "variant": "quickstart",
                            "status": "ready",
                            "label": {"Name": "fraudulent", "Variant": "quickstart"},
                            "features": {
                                "avg_transactions": [
                                    {
                                        "description": "",
                                        "entity": "user",
                                        "name": "avg_transactions",
                                        "owner": "default_user",
                                        "provider": "local-mode",
                                        "data-type": "float32",
                                        "variant": "quickstart",
                                        "status": "ready",
                                        "location": {
                                            "entity": "CustomerID",
                                            "value": "TransactionAmount",
                                            "timestamp": "",
                                        },
                                        "source": {
                                            "Name": "average_user_transaction",
                                            "Variant": "quickstart",
                                        },
                                        "tags": [],
                                        "properties": {},
                                    }
                                ]
                            },
                            "tags": [],
                            "properties": {},
                        }
                    ]
                },
                "tags": [],
                "properties": {},
            }
        ],
    },
    "features": {
        "avg_transactions": [
            {
                "description": "",
                "entity": "user",
                "name": "avg_transactions",
                "owner": "default_user",
                "provider": "local-mode",
                "data-type": "float32",
                "variant": "quickstart",
                "status": "ready",
                "location": {
                    "entity": "CustomerID",
                    "value": "TransactionAmount",
                    "timestamp": "",
                },
                "source": {"Name": "average_user_transaction", "Variant": "quickstart"},
                "tags": [],
                "properties": {},
            }
        ]
    },
    "labels": {},
    "status": "status",
    "serializedConfig": "{}",
    "tags": ["local-mode"],
    "properties": {"resource_type": "Provider"},
}
average_user_transaction = {
    "all-variants": ["quickstart"],
    "type": "Source",
    "default-variant": "quickstart",
    "name": "average_user_transaction",
    "variants": {
        "quickstart": {
            "description": "the average transaction amount for a user",
            "name": "average_user_transaction",
            "source-type": "Source",
            "owner": "default_user",
            "provider": "local-mode",
            "variant": "quickstart",
            "status": "ready",
            "labels": {},
            "features": {
                "avg_transactions": [
                    {
                        "description": "",
                        "entity": "user",
                        "name": "avg_transactions",
                        "owner": "default_user",
                        "provider": "local-mode",
                        "data-type": "float32",
                        "variant": "quickstart",
                        "status": "ready",
                        "location": {
                            "entity": "CustomerID",
                            "value": "TransactionAmount",
                            "timestamp": "",
                        },
                        "source": {
                            "Name": "average_user_transaction",
                            "Variant": "quickstart",
                        },
                        "tags": [],
                        "properties": {},
                    }
                ]
            },
            "training-sets": {
                "fraud_training": [
                    {
                        "description": "",
                        "name": "fraud_training",
                        "owner": "default_user",
                        "variant": "quickstart",
                        "status": "ready",
                        "label": {"Name": "fraudulent", "Variant": "quickstart"},
                        "features": {
                            "avg_transactions": [
                                {
                                    "description": "",
                                    "entity": "user",
                                    "name": "avg_transactions",
                                    "owner": "default_user",
                                    "provider": "local-mode",
                                    "data-type": "float32",
                                    "variant": "quickstart",
                                    "status": "ready",
                                    "location": {
                                        "entity": "CustomerID",
                                        "value": "TransactionAmount",
                                        "timestamp": "",
                                    },
                                    "source": {
                                        "Name": "average_user_transaction",
                                        "Variant": "quickstart",
                                    },
                                    "tags": [],
                                    "properties": {},
                                }
                            ]
                        },
                        "tags": [],
                        "properties": {},
                    }
                ]
            },
            "tags": [],
            "properties": {},
        }
    },
}
user = {
    "description": "",
    "type": "Entity",
    "name": "user",
    "features": {
        "avg_transactions": [
            {
                "description": "",
                "entity": "user",
                "name": "avg_transactions",
                "owner": "default_user",
                "provider": "local-mode",
                "data-type": "float32",
                "variant": "quickstart",
                "status": "ready",
                "location": {
                    "entity": "CustomerID",
                    "value": "TransactionAmount",
                    "timestamp": "",
                },
                "source": {"Name": "average_user_transaction", "Variant": "quickstart"},
                "tags": [],
                "properties": {},
            }
        ]
    },
    "labels": {
        "fraudulent": [
            {
                "description": "",
                "entity": "user",
                "data-type": "bool",
                "name": "fraudulent",
                "owner": "default_user",
                "provider": "",
                "variant": "quickstart",
                "status": "ready",
                "location": {
                    "entity": "CustomerID",
                    "value": "IsFraud",
                    "timestamp": "",
                },
                "source": {"Name": "transactions", "Variant": "quickstart"},
                "training-sets": {
                    "fraud_training": [
                        {
                            "description": "",
                            "name": "fraud_training",
                            "owner": "default_user",
                            "variant": "quickstart",
                            "status": "ready",
                            "label": {"Name": "fraudulent", "Variant": "quickstart"},
                            "features": {
                                "avg_transactions": [
                                    {
                                        "description": "",
                                        "entity": "user",
                                        "name": "avg_transactions",
                                        "owner": "default_user",
                                        "provider": "local-mode",
                                        "data-type": "float32",
                                        "variant": "quickstart",
                                        "status": "ready",
                                        "location": {
                                            "entity": "CustomerID",
                                            "value": "TransactionAmount",
                                            "timestamp": "",
                                        },
                                        "source": {
                                            "Name": "average_user_transaction",
                                            "Variant": "quickstart",
                                        },
                                        "tags": [],
                                        "properties": {},
                                    }
                                ]
                            },
                            "tags": [],
                            "properties": {},
                        }
                    ]
                },
                "tags": [],
                "properties": {},
            }
        ]
    },
    "training-sets": {
        "fraud_training": [
            {
                "description": "",
                "name": "fraud_training",
                "owner": "default_user",
                "variant": "quickstart",
                "status": "ready",
                "label": {"Name": "fraudulent", "Variant": "quickstart"},
                "features": {
                    "avg_transactions": [
                        {
                            "description": "",
                            "entity": "user",
                            "name": "avg_transactions",
                            "owner": "default_user",
                            "provider": "local-mode",
                            "data-type": "float32",
                            "variant": "quickstart",
                            "status": "ready",
                            "location": {
                                "entity": "CustomerID",
                                "value": "TransactionAmount",
                                "timestamp": "",
                            },
                            "source": {
                                "Name": "average_user_transaction",
                                "Variant": "quickstart",
                            },
                            "tags": [],
                            "properties": {},
                        }
                    ]
                },
                "tags": [],
                "properties": {},
            }
        ]
    },
    "status": "ready",
    "tags": [],
    "properties": {},
}
fraudulent = {
    "all-variants": ["quickstart"],
    "type": "Label",
    "default-variant": "quickstart",
    "name": "fraudulent",
    "variants": {
        "quickstart": {
            "description": "",
            "entity": "user",
            "data-type": "bool",
            "name": "fraudulent",
            "owner": "default_user",
            "provider": "",
            "variant": "quickstart",
            "status": "ready",
            "location": {"entity": "CustomerID", "value": "IsFraud", "timestamp": ""},
            "source": {"Name": "transactions", "Variant": "quickstart"},
            "training-sets": {
                "fraud_training": [
                    {
                        "description": "",
                        "name": "fraud_training",
                        "owner": "default_user",
                        "variant": "quickstart",
                        "status": "ready",
                        "label": {"Name": "fraudulent", "Variant": "quickstart"},
                        "features": {
                            "avg_transactions": [
                                {
                                    "description": "",
                                    "entity": "user",
                                    "name": "avg_transactions",
                                    "owner": "default_user",
                                    "provider": "local-mode",
                                    "data-type": "float32",
                                    "variant": "quickstart",
                                    "status": "ready",
                                    "location": {
                                        "entity": "CustomerID",
                                        "value": "TransactionAmount",
                                        "timestamp": "",
                                    },
                                    "source": {
                                        "Name": "average_user_transaction",
                                        "Variant": "quickstart",
                                    },
                                    "tags": [],
                                    "properties": {},
                                }
                            ]
                        },
                        "tags": [],
                        "properties": {},
                    }
                ]
            },
            "tags": [],
            "properties": {},
        }
    },
}
fraud_training = {
    "all-variants": ["quickstart"],
    "type": "TrainingSet",
    "default-variant": "quickstart",
    "name": "fraud_training",
    "variants": {
        "quickstart": {
            "description": "",
            "name": "fraud_training",
            "owner": "default_user",
            "variant": "quickstart",
            "status": "ready",
            "label": {"Name": "fraudulent", "Variant": "quickstart"},
            "features": {
                "avg_transactions": [
                    {
                        "description": "",
                        "entity": "user",
                        "name": "avg_transactions",
                        "owner": "default_user",
                        "provider": "local-mode",
                        "data-type": "float32",
                        "variant": "quickstart",
                        "status": "ready",
                        "location": {
                            "entity": "CustomerID",
                            "value": "TransactionAmount",
                            "timestamp": "",
                        },
                        "source": {
                            "Name": "average_user_transaction",
                            "Variant": "quickstart",
                        },
                        "tags": [],
                        "properties": {},
                    }
                ]
            },
            "tags": [],
            "properties": {},
        }
    },
}


def remove_keys(obj, rubbish):
    if isinstance(obj, dict):
        obj = {
            key: remove_keys(value, rubbish)
            for key, value in obj.items()
            if key not in rubbish
        }
    elif isinstance(obj, list):
        obj = [remove_keys(item, rubbish) for item in obj if item not in rubbish]
    return obj


@pytest.fixture(scope="module", autouse=True)
def setup():
    import subprocess

    apply = subprocess.run(
        [
            "featureform",
            "apply",
            "client/examples/local_quickstart.py",
            "--local",
        ]
    )
    yield apply


@pytest.fixture
def client():
    app.config["TESTING"] = True
    with app.test_client() as client:
        with app.app_context():
            yield client


def check_objs(path, test_obj, client: FlaskClient):
    response = client.get(path)
    assert response.status == "200 OK"
    json_resource = json.loads(response.data.decode())
    removed_created_json = remove_keys(json_resource, ["created", "definition"])
    if isinstance(removed_created_json, dict):
        assert test_obj == removed_created_json
    elif isinstance(removed_created_json, list):
        actual = {obj["name"]: obj for obj in removed_created_json}
        expected = {obj["name"]: obj for obj in test_obj}
        assert actual == expected


@pytest.mark.local
def test_apply_exit_code(setup):
    apply = setup
    assert apply.returncode == 0, f"OUT: {apply.stdout}, ERR: {apply.stderr}"


@pytest.mark.local
def test_version(client: FlaskClient):
    response = client.get("data/version")
    assert response.status == "200 OK"
    assert response.data is not None
    json_obj = json.loads(response.data)
    assert json_obj["version"] is not None


@pytest.mark.local
def test_features(client: FlaskClient):
    check_objs("/data/features", features, client)


@pytest.mark.local
def test_labels(client: FlaskClient):
    check_objs("/data/labels", labels, client)


@pytest.mark.local
def test_sources(client: FlaskClient):
    check_objs("/data/sources", sources, client)


@pytest.mark.local
def test_training_sets(client: FlaskClient):
    check_objs("/data/training_sets", training_sets, client)


@pytest.mark.local
def test_entities(client: FlaskClient):
    check_objs("/data/entities", entities, client)


@pytest.mark.local
def test_models(client: FlaskClient):
    check_objs("/data/models", models, client)


@pytest.mark.local
def test_providers(client: FlaskClient):
    check_objs("/data/providers", providers, client)


@pytest.mark.local
def test_users(client: FlaskClient):
    # wrap object in list
    check_objs("/data/users", [default_user], client)


@pytest.mark.local
def test_feature_avg_transcations(client: FlaskClient):
    # first features item
    check_objs("/data/features/avg_transactions", features[0], client)


@pytest.mark.local
def test_default_user(client: FlaskClient):
    check_objs("/data/users/default_user", default_user, client)


@pytest.mark.local
def test_localmode(client: FlaskClient):
    check_objs("/data/providers/local-mode", localmode, client)


@pytest.mark.local
def test_average_user_transaction(client: FlaskClient):
    check_objs(
        "/data/sources/average_user_transaction", average_user_transaction, client
    )


@pytest.mark.local
def test_fraudulent(client: FlaskClient):
    check_objs("/data/labels/fraudulent", fraudulent, client)


@pytest.mark.local
def test_fraud_training(client: FlaskClient):
    check_objs("/data/training_sets/fraud_training", fraud_training, client)


@pytest.mark.local
def test_user(client: FlaskClient):
    check_objs("/data/entities/user", user, client)


@pytest.mark.local
def test_sourcedata_ok(client: FlaskClient):
    response = client.get(
        "/data/sourcedata",
        query_string=dict(name="average_user_transaction", variant="quickstart"),
    )
    json_data = json.loads(response.data.decode())

    assert response.status == "200 OK"
    assert json_data["columns"] == ["CustomerID", "TransactionAmount"]
    assert json_data["rows"] != None
    assert len(json_data["rows"]) > 1


@pytest.mark.local
def test_sourcedata_empty_args(client: FlaskClient):
    response = client.get(
        "/data/sourcedata",
        query_string=dict(name="", variant=""),
    )
    json_data = json.loads(response.data.decode())

    assert response.status == "400 BAD REQUEST"
    assert (
        json_data
        == "Error 400: GetSourceData - Could not find the name() or variant() query parameters."
    )


@pytest.mark.local
def test_search_metadata(client: FlaskClient):
    search_term = "fraud_training"
    response = client.get("/data/search", query_string=dict(q=search_term))
    json_data = json.loads(response.data.decode())

    # at least more than 1
    assert response.status == "200 OK"
    assert len(json_data) > 0
    assert json_data[0]["Name"] == search_term


@pytest.mark.local
def test_search_metadata_no_results(client: FlaskClient):
    response = client.get("/data/search", query_string=dict(q="not_found"))
    json_data = json.loads(response.data.decode())

    assert response.status == "200 OK"
    assert len(json_data) == 0


@pytest.mark.local
def test_sourcedata_args_not_found(client: FlaskClient):
    response = client.get(
        "/data/sourcedata",
        query_string=dict(name="not_found", variant="not_found"),
    )
    json_data = json.loads(response.data.decode())

    print(json_data)
    assert response.status == "500 INTERNAL SERVER ERROR"
    assert json_data == "Error 500: Unable to retrieve source_data columns."


@pytest.mark.local
def test_get_tags(client: FlaskClient):
    variant_name = "quickstart"
    resource_name = "average_user_transaction"
    response = client.post(
        f"/data/sources/{resource_name}/gettags",
        data=json.dumps(dict(variant=variant_name)),
        content_type="application/json",
    )
    json_data = json.loads(response.data.decode())

    assert response.status == "200 OK"
    assert json_data["name"] == resource_name
    assert json_data["variant"] == variant_name
    assert json_data["tags"] == []


@pytest.fixture(scope="module", autouse=True)
def setup_and_teardown():
    yield
    cleanup()


def cleanup():
    client = featureform.Client(local=True)
    client.impl.db.close()
    try:
        shutil.rmtree(".featureform", onerror=del_rw)
    except FileNotFoundError:
        print("File Already Removed")


def del_rw(action, name, exc):
    os.chmod(name, stat.S_IWRITE)
    os.remove(name)
