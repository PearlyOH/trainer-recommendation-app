import { useState } from 'react';
import { NewHeader } from '../components/NewHeader';
import { NewHeroSection } from '../components/NewHeroSection';
import { TrainerFilter } from '../components/TrainerFilter';
import { NewRecommendationFlow } from '../components/NewRecommendationFlow';
import { Footer } from '../components/Footer';

export function HomePage() {
  const [isRecommendationOpen, setIsRecommendationOpen] = useState(false);

  const handleGetStarted = () => {
    setIsRecommendationOpen(true);
  };

  return (
    <div className="min-h-screen bg-white">
      <NewHeader onGetStarted={handleGetStarted} />
      
      <main>
        <NewHeroSection onGetStarted={handleGetStarted} />
        <TrainerFilter />
      </main>

      <Footer />

      <NewRecommendationFlow 
        isOpen={isRecommendationOpen}
        onClose={() => setIsRecommendationOpen(false)}
      />
    </div>
  );
}