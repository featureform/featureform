import Container from '@mui/material/Container';
import Paper from '@mui/material/Paper';
import React, { useEffect } from 'react';
import Loader from 'react-loader-spinner';
import { connect } from 'react-redux';
import Resource from '../../api/resources/Resource.js';
import NotFoundPage from '../notfoundpage/NotFoundPage';
import { setVariant } from '../resource-list/VariantSlice.js';
import { fetchEntity } from './EntityPageSlice.js';
import EntityPageView from './EntityPageView.js';

const mapDispatchToProps = (dispatch) => {
  return {
    fetch: (api, type, title) => dispatch(fetchEntity({ api, type, title })),
    setVariant: (type, name, variant) =>
      dispatch(setVariant({ type, name, variant })),
  };
};

function mapStateToProps(state) {
  return {
    entityPage: state.entityPage,
    activeVariants: state.selectedVariant,
  };
}

const LoadingDots = () => {
  return (
    <div data-testid='loadingDotsId'>
      <Container maxWidth='xl'>
        <Paper elevation={3}>
          <Container maxWidth='sm'>
            <Loader type='ThreeDots' color='grey' height={40} width={40} />
          </Container>
        </Paper>
      </Container>
    </div>
  );
};

const fetchNotFound = (object) => {
  return !object?.resources?.name && !object?.resources?.type;
};

const EntityPage = ({
  api,
  entityPage,
  activeVariants,
  type,
  entity,
  ...props
}) => {
  let resourceType = Resource[Resource.pathToType[type]];
  const fetchEntity = props.fetch;

  useEffect(async () => {
    if (api && type && entity) {
      await fetchEntity(api, type, entity);
    }
  }, [type, entity]);

  let body = <></>;
  if (entityPage.loading === true) {
    body = <LoadingDots />;
  } else if (entityPage.loading === false) {
    if (entityPage.failed === true || fetchNotFound(entityPage)) {
      body = <NotFoundPage />;
    } else {
      body = (
        <EntityPageView
          api={api}
          entity={entityPage}
          setVariant={props.setVariant}
          activeVariants={activeVariants}
          typePath={type}
          resourceType={resourceType}
        />
      );
    }
  }
  return body;
};

export default connect(mapStateToProps, mapDispatchToProps)(EntityPage);
