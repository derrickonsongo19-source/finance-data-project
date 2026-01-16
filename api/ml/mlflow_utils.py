# api/ml/mlflow_utils.py
"""
MLflow Utilities Module
Helper functions for MLflow experiment tracking and management.
"""
import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient
import os
from datetime import datetime
from typing import Dict, Any, Optional


def setup_mlflow():
    """Setup MLflow tracking"""
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    mlflow.set_tracking_uri(tracking_uri)
    
    # Set experiment name
    experiment_name = "personal_finance_ml"
    mlflow.set_experiment(experiment_name)
    
    print(f"✓ MLflow tracking URI: {tracking_uri}")
    print(f"✓ MLflow experiment: {experiment_name}")
    
    return mlflow


def log_training_run(
    model,
    model_name: str,
    params: Dict[str, Any],
    metrics: Dict[str, float],
    X_test=None,
    y_test=None,
    feature_names=None
):
    """
    Log a model training run to MLflow
    
    Args:
        model: Trained model object
        model_name: Name of the model
        params: Dictionary of parameters
        metrics: Dictionary of metrics
        X_test: Test features (optional)
        y_test: Test labels (optional)
        feature_names: List of feature names (optional)
    """
    with mlflow.start_run(run_name=f"{model_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
        # Log parameters
        for key, value in params.items():
            mlflow.log_param(key, value)
        
        # Log metrics
        for key, value in metrics.items():
            mlflow.log_metric(key, value)
        
        # Log model
        mlflow.sklearn.log_model(model, model_name)
        
        # Log additional info
        mlflow.set_tag("model_type", type(model).__name__)
        mlflow.set_tag("training_date", datetime.now().isoformat())
        
        if feature_names:
            mlflow.log_param("feature_names", feature_names)
        
        print(f"✓ Logged {model_name} to MLflow")


def get_best_model(experiment_name: str, metric: str = "r2_score") -> Optional[Any]:
    """
    Get the best model from MLflow based on a metric
    
    Args:
        experiment_name: Name of the experiment
        metric: Metric to optimize (default: r2_score)
    
    Returns:
        Best model or None
    """
    try:
        client = MlflowClient()
        experiment = client.get_experiment_by_name(experiment_name)
        
        if experiment:
            runs = client.search_runs(
                experiment_ids=[experiment.experiment_id],
                order_by=[f"metrics.{metric} DESC"],
                max_results=1
            )
            
            if runs:
                best_run = runs[0]
                model_uri = f"runs:/{best_run.info.run_id}/model"
                model = mlflow.sklearn.load_model(model_uri)
                print(f"✓ Loaded best model with {metric}: {best_run.data.metrics.get(metric, 'N/A')}")
                return model
        
        print("⚠ No best model found")
        return None
        
    except Exception as e:
        print(f"Error getting best model: {e}")
        return None


def log_prediction(prediction_data: Dict[str, Any], model_name: str = "spending_predictor"):
    """
    Log prediction results to MLflow
    
    Args:
        prediction_data: Dictionary containing prediction results
        model_name: Name of the model used
    """
    try:
        with mlflow.start_run(run_name=f"prediction_{datetime.now().strftime('%H%M%S')}", nested=True):
            mlflow.log_param("model", model_name)
            mlflow.log_param("prediction_timestamp", datetime.now().isoformat())
            
            # Log prediction metrics
            if "accuracy" in prediction_data:
                mlflow.log_metric("prediction_accuracy", prediction_data["accuracy"])
            
            if "confidence" in prediction_data:
                mlflow.log_metric("average_confidence", prediction_data["confidence"])
            
            # Log prediction results
            mlflow.log_dict(prediction_data, "prediction_results.json")
            
            print(f"✓ Logged prediction to MLflow")
            
    except Exception as e:
        print(f"Error logging prediction: {e}")


def list_experiments():
    """List all MLflow experiments"""
    try:
        client = MlflowClient()
        experiments = client.list_experiments()
        
        print("\n" + "="*50)
        print("MLflow Experiments")
        print("="*50)
        
        for exp in experiments:
            print(f"\nExperiment: {exp.name}")
            print(f"  ID: {exp.experiment_id}")
            print(f"  Artifact Location: {exp.artifact_location}")
            print(f"  Lifecycle Stage: {exp.lifecycle_stage}")
        
        return experiments
        
    except Exception as e:
        print(f"Error listing experiments: {e}")
        return []


# Initialize MLflow on import
setup_mlflow()
