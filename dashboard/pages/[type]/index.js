import DataPage from "../components/datapage/DataPage"
import { useRouter } from "next/router";

const DataPageRoute = () => {
    const router = useRouter()
    const { type } = router.query

    return (
      <DataPage type={type} />
    );
  };
  
export default DataPageRoute