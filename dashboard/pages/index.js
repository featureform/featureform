import { NextPage } from 'next'
import Head from 'next/head'
import HomePage from '../src/components/homepage/HomePage'

const IndexPage = () => {
  return (
    <div>
      <Head>
        <title>Featureform Dashboard</title>
        <link rel="icon" href="/static/favicon.ico" />
      </Head>
        <HomePage />
    </div>
  )
}

export default IndexPage