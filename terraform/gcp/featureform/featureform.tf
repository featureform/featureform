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
}

resource "helm_release" "featureform" {
  name = "featureform"
  namespace = "default"

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
    helm_release.certmgr
  ]
}