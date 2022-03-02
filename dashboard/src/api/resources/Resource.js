export default class Resource {
  static pathToType = {
    features: "Feature",
    labels: "Label",
    "training-sets": "TrainingSet",
    "primary-data": "PrimaryData",
    entities: "Entity",
    models: "Model",
    providers: "Provider",
    users: "User",
  };
  static Feature = new Resource(
    "Feature",
    "Features",
    "/features",
    true,
    "description",
    true
  );
  static Label = new Resource(
    "Label",
    "Labels",
    "/labels",
    true,
    "label",
    false
  );
  static TrainingSet = new Resource(
    "Training Set",
    "Training Sets",
    "/training-sets",
    true,
    "storage",
    true
  );
  static PrimaryData = new Resource(
    "Primary Data",
    "Primary Data",
    "/primary-data",
    true,
    "source",
    false
  );
  static Entity = new Resource(
    "Entity",
    "Entities",
    "/entities",
    false,
    "fingerprint",
    false
  );
  static Model = new Resource(
    "Model",
    "Models",
    "/models",
    false,
    "model_training",
    false
  );
  static Provider = new Resource(
    "Provider",
    "Providers",
    "/providers",
    false,
    "device_hub",
    false
  );
  static User = new Resource("User", "Users", "/users", false, "person");
  constructor(
    type,
    typePlural,
    urlPath,
    hasVariants,
    materialIcon,
    hasMetrics
  ) {
    this._type = type;
    this._typePlural = typePlural;
    this._urlPath = urlPath;
    this._hasVariants = hasVariants;
    this._materialIcon = materialIcon;
    this._hasMetrics = hasMetrics;
  }

  get urlPath() {
    return this._urlPath;
  }

  urlPathResource(name) {
    return this._urlPath + "/" + name;
  }

  get hasVariants() {
    return this._hasVariants;
  }

  get typePlural() {
    return this._typePlural;
  }

  get hasMetrics() {
    return this._hasMetrics;
  }

  get materialIcon() {
    return this._materialIcon;
  }

  get type() {
    return this._type;
  }
}
