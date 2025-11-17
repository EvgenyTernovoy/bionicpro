import React, { useEffect, useState } from 'react';

const ReportPage: React.FC = () => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [authenticated, setAuthenticated] = useState<boolean | null>(null);

  const API_URL = process.env.REACT_APP_API_URL;

  //
  // 1. Проверяем текущую сессию при загрузке страницы
  //
  useEffect(() => {
    const checkSession = async () => {
      try {
        const response = await fetch(`${API_URL}/session`, {
          credentials: 'include',
        });

        if (response.ok) {
          setAuthenticated(true);
        } else {
          setAuthenticated(false);
        }
      } catch {
        setAuthenticated(false);
      }
    };

    checkSession();
  }, [API_URL]);

  //
  // 2. Фронтенд логин → редирект на /auth/login 
  //
  const login = () => {
    window.location.href = `${API_URL}/login`;
  };

  //
  // 3. Запрос на скачивание репорта
  //
  const downloadReport = async () => {
    setError(null);
    setLoading(true);

    try {
      const response = await fetch(`${API_URL}/reports`, {
        method: 'GET',
        credentials: 'include', // обязательно! отправляет cookie
      });

      if (response.status === 401) {
        setAuthenticated(false);
        setError('Session expired. Please login again.');
        return;
      }

      if (!response.ok) {
        throw new Error(`Error: ${response.status}`);
      }

      // скачивание файла
      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'report.pdf';
      a.click();
      window.URL.revokeObjectURL(url);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setLoading(false);
    }
  };

  //
  // 4. Первичная загрузка
  //
  if (authenticated === null) {
    return <div>Loading...</div>;
  }

  //
  // 5. Не аутентифицирован — показываем кнопку логина
  //
  if (authenticated === false) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
        <button
          onClick={login}
          className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
        >
          Login
        </button>
      </div>
    );
  }

  // 6. Аутентифицирован — показываем UI
  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
      <div className="p-8 bg-white rounded-lg shadow-md">
        <h1 className="text-2xl font-bold mb-6">Usage Reports</h1>

        <button
          onClick={downloadReport}
          disabled={loading}
          className={`px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600 ${
            loading ? 'opacity-50 cursor-not-allowed' : ''
          }`}
        >
          {loading ? 'Generating Report...' : 'Download Report'}
        </button>

        {error && (
          <div className="mt-4 p-4 bg-red-100 text-red-700 rounded">
            {error}
          </div>
        )}
      </div>
    </div>
  );
};

export default ReportPage;
