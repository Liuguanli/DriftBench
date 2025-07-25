import numpy as np
import pandas as pd

# register table
JOINT_STRATEGY_REGISTRY = {}

def register_joint_strategy(name: str):
    def decorator(cls):
        JOINT_STRATEGY_REGISTRY[name] = cls()
        return cls
    return decorator


class JointSampler:
    def fit(self, df: pd.DataFrame, config: dict = None):
        raise NotImplementedError

    def sample(self, n: int) -> pd.DataFrame:
        raise NotImplementedError

# -----------------------
# Copula Strategy
# -----------------------
@register_joint_strategy("copula")
class CopulaJointSampler(JointSampler):
    def fit(self, df: pd.DataFrame, config: dict = None):
        from copulas.multivariate import GaussianMultivariate
        self.model = GaussianMultivariate()
        self.model.fit(df)

    def sample(self, n: int) -> pd.DataFrame:
        return self.model.sample(n)
    

# -----------------------
# GMM Strategy
# -----------------------
@register_joint_strategy("gmm")
class GMMJointSampler(JointSampler):
    def fit(self, df: pd.DataFrame, config: dict = None):
        from sklearn.mixture import GaussianMixture
        from sklearn.preprocessing import StandardScaler

        self.columns = df.columns
        self.config = config or {}
        self.scaler = StandardScaler()

        # only use numerical values
        numeric_df = df.select_dtypes(include=np.number)
        self.numeric_columns = numeric_df.columns
        self.categorical_columns = [col for col in df.columns if col not in self.numeric_columns]

        self.X_scaled = self.scaler.fit_transform(numeric_df)
        self.model = GaussianMixture(n_components=self.config.get("n_components", 5))
        self.model.fit(self.X_scaled)

        # cache non-numeric values
        self.non_numeric_values = df[self.categorical_columns].sample(n=len(df), random_state=42).reset_index(drop=True)

    def sample(self, n: int) -> pd.DataFrame:
        X_sampled = self.model.sample(n)[0]
        X_inverse = self.scaler.inverse_transform(X_sampled)
        numeric_df = pd.DataFrame(X_inverse, columns=self.numeric_columns)

        if self.categorical_columns:
            # randomly sample n-line non-numerical from cache 
            cat_df = self.non_numeric_values.sample(n=n, replace=True).reset_index(drop=True)
            return pd.concat([numeric_df.reset_index(drop=True), cat_df], axis=1)[self.columns]
        else:
            return numeric_df[self.columns]
