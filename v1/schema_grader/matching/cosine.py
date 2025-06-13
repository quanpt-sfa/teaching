import numpy as np

def cosine_mat(A, B):
    """Tính ma trận cosine similarity giữa hai tập vector.
    
    Args:
        A: Ma trận các vector đầu tiên
        B: Ma trận các vector thứ hai
    
    Returns:
        np.ndarray: Ma trận tương đồng cosine
    """
    A = np.atleast_2d(A)
    B = np.atleast_2d(B)
    A_norm = np.linalg.norm(A, axis=1, keepdims=True) + 1e-8
    B_norm = np.linalg.norm(B, axis=1, keepdims=True) + 1e-8
    A = A / A_norm
    B = B / B_norm
    return A @ B.T
