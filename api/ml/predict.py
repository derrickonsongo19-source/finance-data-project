# api/ml/predict.py
"""
ML Predictor Module
Handles machine learning predictions for the finance API.
This includes spending predictions, categorization, and pattern detection.
"""
import os
import json
import numpy as np
from typing import List, Dict, Any
from datetime import datetime, timedelta
import mlflow
import mlflow.sklearn
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
import pickle
from pathlib import Path

# Import models
from models import SpendingPrediction


class MLPredictor:
    """Machine Learning predictor for financial data"""
    
    def __init__(self):
        """Initialize ML predictor with MLflow tracking"""
        # Set MLflow tracking URI
        mlflow_tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
        
        try:
            mlflow.set_tracking_uri(mlflow_tracking_uri)
            self.mlflow_enabled = True
            print(f"✓ MLflow tracking enabled at {mlflow_tracking_uri}")
        except Exception as e:
            self.mlflow_enabled = False
            print(f"⚠ MLflow disabled: {e}")
        
        # Create models directory
        self.models_dir = Path("ml/models")
        self.models_dir.mkdir(exist_ok=True)
        
        # Initialize models dictionary
        self.models = {}
        self.scalers = {}
        
        # Load or train models
        self._initialize_models()
    
    def _initialize_models(self):
        """Initialize or load ML models"""
        try:
            # Try to load existing models
            self._load_models()
            print("✓ Loaded existing ML models")
        except Exception as e:
            # Train new models if none exist
            print(f"⚠ No existing models found, training new ones... ({e})")
            self._train_initial_models()
    
    def _train_initial_models(self):
        """Train initial models with sample data"""
        try:
            if self.mlflow_enabled:
                with mlflow.start_run(run_name="initial_model_training"):
                    # Log parameters
                    mlflow.log_param("model_type", "linear_regression")
                    mlflow.log_param("training_samples", 100)
                    self._train_models_with_mlflow()
            else:
                self._train_models_without_mlflow()
                
        except Exception as e:
            print(f"⚠ MLflow training failed, using fallback: {e}")
            self._train_models_without_mlflow()
    
    def _train_models_with_mlflow(self):
        """Train models with MLflow tracking"""
        # Create sample training data
        np.random.seed(42)
        
        # Sample data for 4 categories
        categories = ["food", "transport", "entertainment", "shopping"]
        
        for category in categories:
            # Create dummy features (e.g., day of month, month, etc.)
            X_train = np.random.rand(100, 3)  # 100 samples, 3 features
            y_train = np.random.rand(100) * 1000  # Target values
            
            # Scale features
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X_train)
            
            # Train model
            model = LinearRegression()
            model.fit(X_scaled, y_train)
            
            # Save model and scaler
            self.models[category] = model
            self.scalers[category] = scaler
            
            # Log model metrics
            score = model.score(X_scaled, y_train)
            mlflow.log_metric(f"{category}_r2_score", score)
        
        # Save models locally
        self._save_models()
        
        # Log models to MLflow
        for category, model in self.models.items():
            mlflow.sklearn.log_model(model, f"model_{category}")
        
        mlflow.log_artifact("ml/models")
        print("✓ Initial models trained and saved with MLflow")
    
    def _train_models_without_mlflow(self):
        """Train models without MLflow tracking"""
        # Create sample training data
        np.random.seed(42)
        
        # Sample data for 4 categories
        categories = ["food", "transport", "entertainment", "shopping"]
        
        for category in categories:
            # Create dummy features
            X_train = np.random.rand(100, 3)
            y_train = np.random.rand(100) * 1000
            
            # Scale features
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X_train)
            
            # Train model
            model = LinearRegression()
            model.fit(X_scaled, y_train)
            
            # Save model and scaler
            self.models[category] = model
            self.scalers[category] = scaler
        
        # Save models locally
        self._save_models()
        print("✓ Initial models trained and saved (without MLflow)")
    
    def _save_models(self):
        """Save models to disk"""
        for category, model in self.models.items():
            model_path = self.models_dir / f"model_{category}.pkl"
            with open(model_path, 'wb') as f:
                pickle.dump(model, f)
        
        # Save scalers
        scalers_path = self.models_dir / "scalers.pkl"
        with open(scalers_path, 'wb') as f:
            pickle.dump(self.scalers, f)
    
    def _load_models(self):
        """Load models from disk"""
        # Load scalers first
        scalers_path = self.models_dir / "scalers.pkl"
        with open(scalers_path, 'rb') as f:
            self.scalers = pickle.load(f)
        
        # Load models
        model_files = list(self.models_dir.glob("model_*.pkl"))
        for model_file in model_files:
            category = model_file.stem.replace("model_", "")
            with open(model_file, 'rb') as f:
                self.models[category] = pickle.load(f)
    
    def predict_spending(self, historical_spending: Dict[str, float]) -> List[SpendingPrediction]:
        """
        Predict next month's spending for each category
        
        Args:
            historical_spending: Dictionary of category -> amount spent in last period
        
        Returns:
            List of SpendingPrediction objects
        """
        predictions = []
        next_month = (datetime.now() + timedelta(days=30)).strftime("%Y-%m")
        
        try:
            if self.mlflow_enabled:
                with mlflow.start_run(run_name="spending_prediction"):
                    mlflow.log_param("prediction_month", next_month)
                    predictions = self._make_predictions(historical_spending, next_month)
                    mlflow.log_param("total_categories_predicted", len(predictions))
            else:
                predictions = self._make_predictions(historical_spending, next_month)
                
        except Exception as e:
            print(f"⚠ Prediction tracking failed: {e}")
            predictions = self._make_predictions(historical_spending, next_month)
        
        return predictions
    
    def _make_predictions(self, historical_spending: Dict[str, float], next_month: str) -> List[SpendingPrediction]:
        """Make predictions without MLflow tracking"""
        predictions = []
        
        for category, amount in historical_spending.items():
            try:
                # Get model for this category, or use default
                if category in self.models:
                    model = self.models[category]
                    scaler = self.scalers.get(category)
                    
                    # Prepare features (simplified - in reality would use time series features)
                    features = np.array([[amount, 1, 0]])  # Last amount + dummy features
                    
                    if scaler:
                        features_scaled = scaler.transform(features)
                    else:
                        features_scaled = features
                    
                    # Make prediction
                    predicted_amount = model.predict(features_scaled)[0]
                    
                    # Ensure prediction is reasonable (not negative)
                    predicted_amount = max(predicted_amount, amount * 0.5)
                    predicted_amount = min(predicted_amount, amount * 1.5)
                    
                    # Calculate confidence (simplified)
                    confidence = min(0.95, 0.7 + (amount / 1000) * 0.25)
                    
                else:
                    # If no model for this category, use simple forecast
                    predicted_amount = amount  # Same as last period
                    confidence = 0.6  # Lower confidence
                
                # Create prediction object
                prediction = SpendingPrediction(
                    month=next_month,
                    predicted_amount=float(predicted_amount),
                    category=category,
                    confidence=float(confidence)
                )
                
                predictions.append(prediction)
                
            except Exception as e:
                print(f"Error predicting for category {category}: {e}")
                # Fallback prediction
                predictions.append(
                    SpendingPrediction(
                        month=next_month,
                        predicted_amount=float(amount),
                        category=category,
                        confidence=0.5
                    )
                )
        
        return predictions
    
    def categorize_expense(self, description: str, amount: float) -> str:
        """
        Categorize an expense based on description and amount
        
        Args:
            description: Transaction description
            amount: Transaction amount
        
        Returns:
            Predicted category
        """
        # Simple rule-based categorization (could be replaced with ML model)
        description_lower = description.lower()
        
        # Keyword matching
        if any(word in description_lower for word in ["restaurant", "food", "groceries", "supermarket"]):
            return "food"
        elif any(word in description_lower for word in ["uber", "taxi", "bus", "train", "fuel", "gas"]):
            return "transport"
        elif any(word in description_lower for word in ["movie", "netflix", "concert", "game", "entertain"]):
            return "entertainment"
        elif any(word in description_lower for word in ["amazon", "shop", "store", "mall", "purchase"]):
            return "shopping"
        elif any(word in description_lower for word in ["electric", "water", "internet", "phone", "utility"]):
            return "utilities"
        elif any(word in description_lower for word in ["doctor", "hospital", "pharmacy", "medical"]):
            return "health"
        elif amount > 0:  # Positive amount likely income
            return "income"
        else:
            return "other"
    
    def detect_anomalies(self, transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Detect anomalous transactions
        
        Args:
            transactions: List of transaction dictionaries
        
        Returns:
            List of anomalous transactions with reason
        """
        anomalies = []
        
        if not transactions:
            return anomalies
        
        # Calculate statistics
        amounts = [t.get("amount", 0) for t in transactions]
        if amounts:
            mean_amount = np.mean(amounts)
            std_amount = np.std(amounts)
            
            # Detect anomalies (more than 2 standard deviations from mean)
            for transaction in transactions:
                amount = transaction.get("amount", 0)
                if abs(amount - mean_amount) > 2 * std_amount:
                    anomaly = transaction.copy()
                    anomaly["anomaly_reason"] = f"Amount ({amount}) is unusual (mean: {mean_amount:.2f}, std: {std_amount:.2f})"
                    anomalies.append(anomaly)
        
        return anomalies

# Create a singleton instance
ml_predictor = MLPredictor()
