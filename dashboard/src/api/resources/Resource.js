// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

export default class Resource {
  static Feature = new Resource({
    type: 'Feature',
    typePlural: 'Features',
    urlPath: 'features',
    hasVariants: true,
    materialIcon: 'description',
    hasMetrics: true,
  });

  static Label = new Resource({
    type: 'Label',
    typePlural: 'Labels',
    urlPath: 'labels',
    hasVariants: true,
    materialIcon: 'label',
    hasMetrics: false,
  });
  static TrainingSet = new Resource({
    type: 'TrainingSet',
    typePlural: 'Training Sets',
    urlPath: 'training-sets',
    hasVariants: true,
    materialIcon: 'storage',
    hasMetrics: true,
  });
  static Source = new Resource({
    type: 'Source',
    typePlural: 'Datasets',
    displayText: 'Dataset',
    urlPath: 'sources',
    hasVariants: true,
    materialIcon: 'source',
    hasMetrics: false,
  });
  static Entity = new Resource({
    type: 'Entity',
    typePlural: 'Entities',
    urlPath: 'entities',
    hasVariants: false,
    materialIcon: 'fingerprint',
    hasMetrics: false,
  });
  static Model = new Resource({
    type: 'Model',
    typePlural: 'Models',
    urlPath: 'models',
    hasVariants: false,
    materialIcon: 'model_training',
    hasMetrics: false,
  });
  static Provider = new Resource({
    type: 'Provider',
    typePlural: 'Providers',
    urlPath: 'providers',
    hasVariants: false,
    materialIcon: 'device_hub',
    hasMetrics: false,
  });
  static User = new Resource({
    type: 'User',
    typePlural: 'Users',
    urlPath: 'users',
    hasVariants: false,
    materialIcon: 'person',
    hasMetrics: false,
  });

  static _generatePaths() {
    let _pathToType = {};
    Object.entries(Resource).forEach((res) => {
      if (res[1] instanceof Resource) {
        _pathToType[res[1]._urlPath] = res[0];
      }
    });
    return _pathToType;
  }

  static _generateNames() {
    let _pathToType = {};
    Object.entries(Resource).forEach((res) => {
      if (res[1] instanceof Resource) {
        _pathToType[res[1]._type] = res[0];
      }
    });
    return _pathToType;
  }

  static pathToType = this._generatePaths();

  static typeToName = this._generateNames();

  static get resourceTypes() {
    return Object.entries(Resource)
      .filter((res) => res[1] instanceof Resource)
      .map((res) => res[0]);
  }

  constructor(config) {
    this._type = config.type;
    this._typePlural = config.typePlural;
    this._urlPath = config.urlPath;
    this._hasVariants = config.hasVariants;
    this._materialIcon = config.materialIcon;
    this._hasMetrics = config.hasMetrics;
    this._displayText = config.displayText || config.type;
  }

  get urlPath() {
    return '/' + this._urlPath;
  }

  urlPathResource(name) {
    return '/' + this._urlPath + '/' + name;
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

  get displayText() {
    return this._displayText;
  }
}
