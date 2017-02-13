# Do not use anything outside of the standard distribution of python
# when implementing this class
import math

class LogisticRegressionSGD:
    """
    Logistic regression with stochastic gradient descent
    """

    def __init__(self, eta, mu, n_feature):
        """
        Initialization of model parameters
        """
        self.eta = eta
        self.mu = mu
        self.weight = [0.0] * n_feature

    def fit(self, X, y):
        """
        Update model using a pair of training sample
        """

        yhat = self.predict_prob(X)
        for i in X:
            grad = y - (yhat * i[1])
            self.weight[i[0]] = self.weight[i[0]] + self.eta * grad + self.eta * self.mu * self.weight[i[0]]

    def predict(self, X):
        """
        Predict 0 or 1 given X and the current weights in the model
        """
        return 1 if self.predict_prob(X) > 0.5 else 0

    def predict_prob(self, X):
        """
        Sigmoid function
        """

        return 1.0 / (1.0 + math.exp(-math.fsum((self.weight[f]*v for f, v in X))))
