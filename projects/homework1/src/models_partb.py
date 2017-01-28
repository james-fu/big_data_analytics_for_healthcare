import numpy as np
from sklearn.datasets import load_svmlight_file
from sklearn.linear_model import LogisticRegression
from sklearn.svm import LinearSVC
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import *

import utils

# setup the randoms tate
RANDOM_STATE = 545510477


#input: X_train, Y_train
#output: Y_pred
def logistic_regression_pred(X_train, Y_train):
    #train a logistic regression classifier using X_train and Y_train. Use this to predict labels of X_train
    #use default params for the classifier
    np.random.seed(RANDOM_STATE)

    clf = LogisticRegression()
    clf.fit(X_train, Y_train)

    return clf.predict(X_train)

#input: X_train, Y_train
#output: Y_pred
def svm_pred(X_train, Y_train):
    #train a SVM classifier using X_train and Y_train. Use this to predict labels of X_train
    #use default params for the classifier
    np.random.seed(RANDOM_STATE)
    clf = LinearSVC()
    clf.fit(X_train, Y_train)

    return clf.predict(X_train)

#input: X_train, Y_train
#output: Y_pred
def decisionTree_pred(X_train, Y_train):
    #train a logistic regression classifier using X_train and Y_train. Use this to predict labels of X_train
    #use max_depth as 5
    np.random.seed(RANDOM_STATE)
    clf = DecisionTreeClassifier(max_depth=5)
    clf.fit(X_train, Y_train)

    return clf.predict(X_train)

#input: Y_pred,Y_true
#output: accuracy, auc, precision, recall, f1-score
def classification_metrics(Y_pred, Y_true):
    #NOTE: It is important to provide the output in the same order
    print Y_pred.shape, Y_true.shape
    print (Y_pred - Y_true).sum()
    print Y_pred

    return (accuracy_score(Y_true, Y_pred),
            roc_auc_score(Y_true, Y_pred),
            precision_score(Y_true, Y_pred),
            recall_score(Y_true, Y_pred),
            f1_score(Y_true, Y_pred))

#input: Name of classifier, predicted labels, actual labels
def display_metrics(classifierName,Y_pred,Y_true):
    print "______________________________________________"
    print "Classifier: "+classifierName
    acc, auc_, precision, recall, f1score = classification_metrics(Y_pred,Y_true)
    print "Accuracy: "+str(acc)
    print "AUC: "+str(auc_)
    print "Precision: "+str(precision)
    print "Recall: "+str(recall)
    print "F1-score: "+str(f1score)
    print "______________________________________________"
    print ""


def main():
    X_train, Y_train = utils.get_data_from_svmlight("../deliverables/features_svmlight.train")

    display_metrics("Logistic Regression",logistic_regression_pred(X_train,Y_train),Y_train)
    display_metrics("SVM",svm_pred(X_train,Y_train),Y_train)
    display_metrics("Decision Tree",decisionTree_pred(X_train,Y_train),Y_train)


if __name__ == "__main__":
    main()

