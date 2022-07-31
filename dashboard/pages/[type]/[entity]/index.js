import EntityPage from "../../components/entitypage/EntityPage"
import { useRouter } from "next/router";

const EntityPageRoute = () => {
    const router = useRouter()
    const { type, entity } = router.query

    return (
      <EntityPage type={type} entity={entity}/>
    );
  };
  
export default EntityPageRoute