import numpy as np

def cosine_mat(A, B):
    A = np.atleast_2d(A)
    B = np.atleast_2d(B)
    A_norm = np.linalg.norm(A, axis=1, keepdims=True) + 1e-8
    B_norm = np.linalg.norm(B, axis=1, keepdims=True) + 1e-8
    A = A / A_norm
    B = B / B_norm
    return A @ B.T

def safe_stack(vecs):
    arr = np.vstack(vecs)
    if arr.ndim == 1:
        arr = arr.reshape(1, -1)
    return arr
