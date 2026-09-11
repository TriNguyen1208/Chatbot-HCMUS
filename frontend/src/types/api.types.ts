export interface ApiResponse<T> {
  message?: string;
  data: T;
  statusCode?: number;
}

export interface PaginationParams {
  limit?: number;
  cursor_id?: string;
  search?: string;
  type?: string;
}
