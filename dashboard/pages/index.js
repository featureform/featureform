import Head from 'next/head';
import React from 'react';
import HomePage from '../src/components/homepage/HomePage';

const IndexPage = () => {
  return (
    <div>
      <Head>
        <title>Featureform Dashboard</title>
      </Head>
      <HomePage />
    </div>
  );
};

export default IndexPage;
