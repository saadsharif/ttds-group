import logo from './logo.svg';
import './App.css';
import { useState, useEffect } from 'react';
import { ReactSearchKit, SearchBar } from 'react-searchkit';
import axios from 'axios';
import SearchAPI from './api/api'

const searchApi = new SearchAPI()

function App() {

  const [config, setConfig] = useState({});
  const [isLoading, setLoading] = useState(true);

  //read the config - we need before we can load anything
  useEffect(() => {
    getConfig()
  }, [])

  const getConfig = () => {
    axios.get('/config.json').then(response => {
      setConfig(response.data)
      setLoading(false)
    })
  }

  if (isLoading) {
    return <div className="App">Loading...</div>;
  }

  return (
    <ReactSearchKit searchApi={searchApi}>
      
      <div style={{ margin: '2em auto', width: '50%' }}>
      <h1>Our Appplication Name</h1>
          <SearchBar />
        </div>
    </ReactSearchKit>
  );
}

export default App;
