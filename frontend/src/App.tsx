import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { lazy, Suspense } from 'react';
import { NeonColorProvider } from './context/NeonColorContext';
import { ErrorBoundary } from './components/ErrorBoundary';
import Navbar from './components/Navbar';
import ScrollLine from './components/ScrollLine';
import RandomButton from './components/RandomButton';
import BackButton from './components/BackButton';
import AgeVerification from './components/AgeVerification';
import Loading from './components/Loading';

// Lazy load pages for code splitting
const Home = lazy(() => import('./pages/Home'));
const Settings = lazy(() => import('./pages/Settings'));
const CastVideos = lazy(() => import('./pages/CastVideos'));
const Casts = lazy(() => import('./pages/Casts'));
const CategoryVideos = lazy(() => import('./pages/CategoryVideos'));
const Categories = lazy(() => import('./pages/Categories'));
const StudioVideos = lazy(() => import('./pages/StudioVideos'));
const Studios = lazy(() => import('./pages/Studios'));
const SeriesVideos = lazy(() => import('./pages/SeriesVideos'));
const Series = lazy(() => import('./pages/Series'));
const Calendar = lazy(() => import('./pages/Calendar'));
const SearchResults = lazy(() => import('./pages/SearchResults'));
const VideoDetail = lazy(() => import('./pages/VideoDetail'));
const Bookmarks = lazy(() => import('./pages/Bookmarks'));
const LikedVideos = lazy(() => import('./pages/LikedVideos'));

function App() {
  return (
    <ErrorBoundary>
      <BrowserRouter>
        <NeonColorProvider>
          <div className="min-h-screen bg-background">
            <AgeVerification />
            <Navbar />
            <ScrollLine />
            <RandomButton />
            <BackButton />
            <main className="pt-20 pl-12">
              <Suspense fallback={
                <div className="fixed inset-0 flex items-center justify-center bg-background z-40">
                  <Loading size="lg" />
                </div>
              }>
                <Routes>
                  <Route path="/" element={<Home />} />
                  <Route path="/settings" element={<Settings />} />
                  <Route path="/bookmarks" element={<Bookmarks />} />
                  <Route path="/liked" element={<LikedVideos />} />
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
              </Suspense>
            </main>
          </div>
        </NeonColorProvider>
      </BrowserRouter>
    </ErrorBoundary>
  );
}

export default App;
