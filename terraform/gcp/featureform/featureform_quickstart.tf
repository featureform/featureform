resource "kubernetes_namespace" "featureform_quickstart_namespace" {
  count = var.install_quickstart == true ? 1 : 0
  metadata {
    name = "featureform-quickstart"
  }

  depends_on = [
    helm_release.featureform, 
    null_resource.target_group_depends_on
  ]
}

resource "helm_release" "featureform-quickstart" {
  name = "featureform-quickstart"
  namespace = "featureform-quickstart"
  version = var.quickstart_version
  count = var.install_quickstart == true ? 1 : 0

  repository = "https://storage.googleapis.com/featureform-helm/"
  chart = "quickstart"

  depends_on = [
    helm_release.featureform, 
    null_resource.target_group_depends_on
  ]
}