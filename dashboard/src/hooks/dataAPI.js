let hostname = 'localhost';
let port = 3000;
if (typeof window !== 'undefined') {
  hostname = window.location.hostname;
  port = window.location.port;
}
var API_URL = '//' + hostname + ':' + port;
if (process.env.REACT_APP_API_URL) {
  API_URL = process.env.REACT_APP_API_URL.trim();
}

//if you want to override the api url (in any environment local to your machine, set this in ".env.local")
//.env.local is not tracked in source
if (process.env.NEXT_PUBLIC_REACT_APP_API_URL) {
  API_URL = process.env.NEXT_PUBLIC_REACT_APP_API_URL.trim();
}

export function useDataAPI() {
  const getTags = async (type = '', resourceName = '', variant = '') => {
    const address = `${API_URL}/data/${type}/${resourceName}/gettags`;
    const result = await fetch(address, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ variant: variant }),
    })
      .then((res) => res.json())
      .catch((error) => {
        console.error(error);
        return error;
      });
    return result;
  };

  const postTags = async (
    type = '',
    resourceName = '',
    variant = '',
    tagList = []
  ) => {
    const address = `${API_URL}/data/${type}/${resourceName}/tags`;
    const result = await fetch(address, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ tags: tagList, variant: variant }),
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
