import { useNavigate, useLocation } from 'react-router-dom';
import { ArrowLeft } from 'lucide-react';
import { useNeonColor } from '@/context/NeonColorContext';

export default function BackButton() {
  const navigate = useNavigate();
  const location = useLocation();
  const { color } = useNeonColor();

  // Only show on pages other than the main page
  if (location.pathname === '/') {
    return null;
  }

  const handleClick = () => {
    navigate(-1);
  };

  return (
    <button
      onClick={handleClick}
      className="fixed bottom-6 left-6 z-50 p-2.5 bg-transparent rounded-full transition-all duration-300 hover:scale-110 cursor-pointer group -translate-x-1/2"
      style={{
        borderWidth: 1,
        borderColor: color.hex,
        color: color.hex,
        boxShadow: `0 0 10px rgba(${color.rgb}, 0.3)`
      }}
      onMouseEnter={(e) => {
        e.currentTarget.style.boxShadow = `0 0 18px rgba(${color.rgb}, 0.5)`;
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.boxShadow = `0 0 10px rgba(${color.rgb}, 0.3)`;
      }}
      title="Go Back"
    >
      <ArrowLeft className="w-4 h-4 group-hover:-translate-x-0.5 transition-transform duration-300" />
    </button>
  );
}
