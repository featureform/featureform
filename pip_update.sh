POSITIONAL_ARGS=()

pip3 uninstall featureform -y
rm -r client/dist/*

# -n or --nodash parameter has the rebuild only rebuild the client
CLIENT_ONLY=false
while [[ $# -gt 0 ]]; do
  case $1 in
    -n|--nodash)   
      shift # past argument
      shift # past value
      CLIENT_ONLY=true
      ;;
    -*|--*)
      echo "Unknown option $1"
      exit 1
      ;;
  esac
done

if [ "$CLIENT_ONLY" = false ] ; then
    cd dashboard && npm run build
    cd ../
    mkdir -p client/src/featureform/dashboard/
    cp -r dashboard/out client/src/featureform/dashboard/
fi

python3 -m build ./client/
pip3 install client/dist/*.whl