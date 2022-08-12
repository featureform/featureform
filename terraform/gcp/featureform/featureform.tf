resource "null_resource" "target_group_depends_on" {
  triggers = {
    # The reference to the variable here creates an implicit
    # dependency on the variable.
    dependency = var.gcp_services_dependency
  }
}


provider "helm" {
  kubernetes {
    config_path = "~/.kube/config"
  }
}

resource "helm_release" "certmgr" {
  name = "cert-mgr"
  namespace = "default"
  version = var.cert_manager_version

  repository = "https://charts.jetstack.io"
  chart = "cert-manager"
  
  set {
    name  = "installCRDs"
    value = "true"
  }

  depends_on = [
    null_resource.target_group_depends_on
  ]
}

resource "helm_release" "featureform" {
  name = "featureform"
  namespace = "default"
  version = "v0.1.0"

  repository = "https://storage.googleapis.com/featureform-helm/"
  chart = "featureform"
  
  set {
    name  = "global.hostname"
    value = var.featureform_hostname
  }
  
  set {
    name  = "global.publicCert"
    value = var.featureform_public_cert
  }

  depends_on = [
    null_resource.target_group_depends_on,
    helm_release.certmgr
  ]
}