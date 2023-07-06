export function useDataAPI() {
  const getTags = async (type = '', resourceName = '') => {
    const address = `http://localhost/data/${type}/${resourceName}/tags`;
    await fetch(url);
    const result = await fetch(address, {
      headers: {
        'Content-Type': 'application/json',
      },
    })
      .then((res) => res.json())
      .catch((error) => {
        console.error(error);
        return error;
      });
    return result;
  };

  const postTags = async (type = '', resourceName = '', tagList = []) => {
    const address = `http://localhost/data/${type}/${resourceName}/tags`;
    const result = await fetch(address, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ tags: tagList }),
    })
      .then((res) => res.json())
      .catch((error) => {
        console.error(error);
        return error;
      });
    return result;
  };

  return {
    getTags,
    postTags,
  };
}
