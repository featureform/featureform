import DataPage from "../../src/components/datapage/DataPage"
import { useRouter } from "next/router";
import ResourcesAPI from "../../src/api/resources/Resources";
import EntityPage from "components/entitypage/EntityPage";


const DataPageRoute = () => {
  const router = useRouter()
  const { type, entity } = router.query
  const apiHandle = new ResourcesAPI();
  console.log('testType route! data page route. type:', type); //type is "/entitities" ? tf?
  console.log('api present?', apiHandle);
  console.log('\n\n');

  if (type && entity) {
    return (<EntityPage api={apiHandle} type={type} entity={entity} />);
  }
  else {
    return (<DataPage api={apiHandle} type={type} />);
  }
};

export default DataPageRoute