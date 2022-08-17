import DataPage from "../../src/components/datapage/DataPage"
import { useRouter } from "next/router";
import ResourcesAPI from "../../src/api/resources/Resources";


const DataPageRoute = () => {
    const router = useRouter()
    const { type } = router.query
    const apiHandle = new ResourcesAPI();

    return (
      <DataPage api={apiHandle} type={type} />
    );
  };
  
export default DataPageRoute