import React from "react";
import MaterialTable from "material-table";

const FeatureSetList = ({ data }) => {
  const initRes = data || [];
  const copy = (res) => res.map((o) => ({ ...o }));
  // MaterialTable can't handle immutable object, we have to make a copy
  // https://github.com/mbrn/material-table/issues/666
  const mutableRes = copy(initRes);
  return (
    <MaterialTable
      title="Feature Sets"
      columns={[
        { title: "Name", field: "name" },
        { title: "Created", field: "created" },
      ]}
      data={mutableRes}
      options={{
        search: true,
        draggable: false,
      }}
    />
  );
};

export default FeatureSetList;
