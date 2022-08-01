import EntityPage from "../../components/entitypage/EntityPage"
import { useRouter } from "next/router";
import ResourcesAPI from "../../api/resources/Resources";


const EntityPageRoute = () => {
    const router = useRouter()
    const { type, entity } = router.query
    const apiHandle = new ResourcesAPI();

    return (
      <EntityPage api={apiHandle} type={type} entity={entity}/>
    );
  };
  
export default EntityPageRoute