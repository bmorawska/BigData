import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import xgboost as xgb
from sklearn.ensemble import BaggingClassifier, RandomForestClassifier, AdaBoostClassifier, GradientBoostingClassifier
from sklearn.metrics import accuracy_score, confusion_matrix, classification_report, plot_confusion_matrix
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeClassifier


def do_classification(df, reduceColumns):
    y = df['FlightDelay']
    x = df.drop(['FlightDelay'], axis=1)

    if reduceColumns:
        x = x.drop(['WheelsOff', 'WheelsOn', 'TaxiIn', 'ActualElapsedTime', 'DepDelay'], axis=1)

    x_test, x_train, y_test, y_train = train_test_split(x, y, test_size=0.3, random_state=42)

    regular_tree(x, y, x_test, y_test, x_train, y_train)
    bagged_tree(x, y, x_test, y_test, x_train, y_train)
    random_forest(x, y, x_test, y_test, x_train, y_train)
    xgboost(x, y, x_test, y_test, x_train, y_train)
    adaboost(x, y, x_test, y_test, x_train, y_train)
    gradient_boosted_tree(x, y, x_test, y_test, x_train, y_train)

def regular_tree(x, y, x_test, y_test, x_train, y_train):
    regular_tree_classifier = DecisionTreeClassifier(criterion='gini', max_depth=5, class_weight="balanced")
    regular_tree_classifier.fit(x_train, y_train)

    pred_y = regular_tree_classifier.predict(x_test)

    plot_confusion_matrix(regular_tree_classifier, x, y, values_format='.3g', normalize='true')
    plt.show()

    plot_confusion_matrix(regular_tree_classifier, x, y, values_format='.3g', normalize='all')
    plt.show()

    print('\nResults (Regular Tree)')
    print(classification_report(y_test, pred_y))


def bagged_tree(x, y, x_test, y_test, x_train, y_train):
    bagged_tree_classifier = BaggingClassifier(
        DecisionTreeClassifier(criterion='gini', max_depth=5, class_weight="balanced"),
        n_estimators=20)
    bagged_tree_classifier.fit(x_train, y_train)

    pred_y = bagged_tree_classifier.predict(x_test)

    plot_confusion_matrix(bagged_tree_classifier, x, y, values_format='.3g', normalize='true')
    plt.show()

    plot_confusion_matrix(bagged_tree_classifier, x, y, values_format='.3g', normalize='all')
    plt.show()

    print('\nResults (Bagged Tree)')
    print(classification_report(y_test, pred_y))


def random_forest(x, y, x_test, y_test, x_train, y_train):
    random_forest_classifier = RandomForestClassifier(n_estimators=100, max_depth=5, class_weight='balanced_subsample')
    random_forest_classifier.fit(x_train, y_train)

    pred_y = random_forest_classifier.predict(x_test)

    print('\nConfusion Matrix (Random Forest)')

    plot_confusion_matrix(random_forest_classifier, x, y, values_format='.3g', normalize='true')
    plt.show()

    plot_confusion_matrix(random_forest_classifier, x, y, values_format='.3g', normalize='all')
    plt.show()

    print('\nResults (Random Forest)')
    print(classification_report(y_test, pred_y))


def xgboost(x, y, x_test, y_test, x_train, y_train):
    xgboost_classifier = xgb.XGBClassifier(use_label_encoder=False, eval_metric='error', learning_rate=0.1, max_depth=5,
                                       subsample=0.7, n_estimators=50)
    xgboost_classifier.fit(x_train, y_train)

    pred_y = xgboost_classifier.predict(x_test)

    print('\nConfusion Matrix (XGBoost)')
    plot_confusion_matrix(xgboost_classifier, x, y, values_format='.3g', normalize='true')
    plt.show()

    plot_confusion_matrix(xgboost_classifier, x, y, values_format='.3g', normalize='all')
    plt.show()

    print('\nResults (XGBoost)')
    print(classification_report(y_test, pred_y))


def adaboost(x, y, x_test, y_test, x_train, y_train):
    adaboost_classifier = AdaBoostClassifier(random_state=42)
    adaboost_classifier.fit(x_train, y_train)

    pred_y = adaboost_classifier.predict(x_test)

    print('\nConfusion Matrix (AdaBoost)')
    plot_confusion_matrix(adaboost_classifier, x, y, values_format='.3g', normalize='true')
    plt.show()

    plot_confusion_matrix(adaboost_classifier, x, y, values_format='.3g', normalize='all')
    plt.show()

    print('\nResults (AdaBoost)')
    print(classification_report(y_test, pred_y))

def gradient_boosted_tree(x, y, x_test, y_test, x_train, y_train):
    grad_boosted_trees = GradientBoostingClassifier(random_state=42)
    grad_boosted_trees.fit(x_train, y_train)

    pred_y = grad_boosted_trees.predict(x_test)

    print('\nConfusion Matrix (Gradient Boosted Trees)')
    plot_confusion_matrix(grad_boosted_trees, x, y, values_format='.3g', normalize='true')
    plt.show()

    plot_confusion_matrix(grad_boosted_trees, x, y, values_format='.3g', normalize='all')
    plt.show()

    print('\nResults (Gradient Boosted Trees)')
    print(classification_report(y_test, pred_y))
