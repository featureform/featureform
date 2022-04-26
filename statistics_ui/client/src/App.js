import logo from './logo.svg';
import React from 'react';
import './App.css';
import { BrowserRouter, Switch, Route } from "react-router-dom";
import Metrics from './components/determine_component';

class App extends React.Component {
  render() {
    return (
    
        <BrowserRouter>
          <Switch>
            <Route path="/:columnname" component={Metrics} />
          </Switch>
        </BrowserRouter>

    );
  }
}

export default App
