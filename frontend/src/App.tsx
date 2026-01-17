import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { NeonColorProvider } from './context/NeonColorContext';
import Navbar from './components/Navbar';
import ScrollLine from './components/ScrollLine';
import RandomButton from './components/RandomButton';
import BackButton from './components/BackButton';
import AgeVerification from './components/AgeVerification';
import Home from './pages/Home';
import Settings from './pages/Settings';
import CastVideos from './pages/CastVideos';
import Casts from './pages/Casts';
import CategoryVideos from './pages/CategoryVideos';
import Categories from './pages/Categories';
import StudioVideos from './pages/StudioVideos';
import Studios from './pages/Studios';
import SeriesVideos from './pages/SeriesVideos';
import Series from './pages/Series';
import Calendar from './pages/Calendar';
import SearchResults from './pages/SearchResults';
import VideoDetail from './pages/VideoDetail';
import Bookmarks from './pages/Bookmarks';

function App() {
  return (
    <BrowserRouter>
      <NeonColorProvider>
        <div className="min-h-screen bg-background dark">
          <AgeVerification />
          <Navbar />
          <ScrollLine />
          <RandomButton />
          <BackButton />
          <main className="pt-20">
            <Routes>
              <Route path="/" element={<Home />} />
              <Route path="/settings" element={<Settings />} />
              <Route path="/bookmarks" element={<Bookmarks />} />
              <Route path="/calendar" element={<Calendar />} />
              <Route path="/categories" element={<Categories />} />
              <Route path="/casts" element={<Casts />} />
              <Route path="/studios" element={<Studios />} />
              <Route path="/series" element={<Series />} />
              <Route path="/search" element={<SearchResults />} />
              <Route path="/cast/:name" element={<CastVideos />} />
              <Route path="/category/:name" element={<CategoryVideos />} />
              <Route path="/studio/:name" element={<StudioVideos />} />
              <Route path="/series/:name" element={<SeriesVideos />} />
              <Route path="/video/:code" element={<VideoDetail />} />
            </Routes>
          </main>
        </div>
      </NeonColorProvider>
    </BrowserRouter>
  );
}

export default App;
