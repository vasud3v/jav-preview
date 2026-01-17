import { useState, useEffect } from 'react';
import { AlertCircle } from 'lucide-react';

const STORAGE_KEY = 'age_verified';

export default function AgeVerification() {
  const [show, setShow] = useState(false);
  const [isExiting, setIsExiting] = useState(false);

  useEffect(() => {
    const verified = localStorage.getItem(STORAGE_KEY);
    if (!verified) {
      setShow(true);
      document.body.style.overflow = 'hidden';
    }
    return () => {
      document.body.style.overflow = '';
    };
  }, []);

  const handleEnter = () => {
    localStorage.setItem(STORAGE_KEY, 'true');
    setIsExiting(true);
    setTimeout(() => {
      setShow(false);
      document.body.style.overflow = '';
    }, 400);
  };

  const handleExit = () => {
    window.location.href = 'https://www.google.com';
  };

  if (!show) return null;

  return (
    <div 
      className={`fixed inset-0 z-[9999] flex items-center justify-center p-4 transition-all duration-400 ${
        isExiting ? 'opacity-0 backdrop-blur-0' : 'opacity-100 backdrop-blur-md'
      }`}
      style={{ backgroundColor: 'rgba(0,0,0,0.92)' }}
    >
      {/* Subtle ambient glow */}
      <div className="absolute inset-0 overflow-hidden pointer-events-none">
        <div 
          className={`absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[600px] h-[600px] rounded-full transition-opacity duration-700 ${
            isExiting ? 'opacity-0' : 'opacity-100'
          }`}
          style={{
            background: 'radial-gradient(circle, rgba(239,68,68,0.08) 0%, transparent 70%)',
          }}
        />
      </div>

      {/* Modal */}
      <div 
        className={`relative w-full max-w-sm transition-all duration-400 ${
          isExiting ? 'opacity-0 scale-95 translate-y-4' : 'opacity-100 scale-100 translate-y-0'
        }`}
      >
        <div className="bg-zinc-950 rounded-2xl border border-zinc-800/80 shadow-2xl overflow-hidden">
          {/* Top accent line */}
          <div className="h-1 bg-gradient-to-r from-red-500 via-red-600 to-red-500" />
          
          {/* Content */}
          <div className="p-8">
            {/* Icon */}
            <div className="flex justify-center mb-6">
              <div className="w-16 h-16 rounded-2xl bg-red-500/10 border border-red-500/20 flex items-center justify-center">
                <AlertCircle className="w-8 h-8 text-red-500" />
              </div>
            </div>

            {/* Text */}
            <div className="text-center mb-8">
              <h1 className="text-xl font-semibold text-white mb-3">
                Age Restricted Content
              </h1>
              <p className="text-zinc-400 text-sm leading-relaxed">
                This website contains adult material. You must be 
                <span className="text-white font-medium"> 18 years or older</span> to enter.
              </p>
            </div>

            {/* Buttons */}
            <div className="space-y-3">
              <button
                onClick={handleEnter}
                className="w-full h-12 bg-white text-black font-medium rounded-xl hover:bg-zinc-200 active:scale-[0.98] transition-all duration-200 cursor-pointer"
              >
                I am 18 or older
              </button>
              
              <button
                onClick={handleExit}
                className="w-full h-12 bg-zinc-900 text-zinc-400 font-medium rounded-xl border border-zinc-800 hover:bg-zinc-800 hover:text-zinc-300 hover:border-zinc-700 active:scale-[0.98] transition-all duration-200 cursor-pointer"
              >
                Exit
              </button>
            </div>
          </div>

          {/* Footer */}
          <div className="px-8 pb-6">
            <p className="text-zinc-600 text-[11px] text-center leading-relaxed">
              By entering, you confirm you are of legal age and accept our terms.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
