import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import ProviderTable from './ProviderTable';
import { providerList } from './test_data';

jest.mock('next/router', () => ({
  useRouter: () => ({
    push: jest.fn(),
  }),
}));

jest.mock('../../../hooks/dataAPI', () => ({
  useDataAPI: () => {
    return {
      getProviders: jest.fn().mockResolvedValue(providerList),
      isLoading: false,
      error: null,
    };
  },
}));

describe('ProviderTable', () => {
  test('renders provider table with data', async () => {
    const helper = render(<ProviderTable />);

    const foundPostgres = await helper.findByText('quickstart-postgres');
    const foundRedis = helper.getByText('quickstart-redis');
    expect(foundPostgres).toBeInTheDocument();
    expect(foundRedis).toBeInTheDocument();
  });

  test.skip('renders the status correctly', async () => {
    render(<ProviderTable />);

    const statuses = await screen.findAllByText('Status: Connected');
    const expectedConnected = providerList.data.filter(
      (p) => p.status === 'Connected'
    ).length;
    expect(statuses).toHaveLength(expectedConnected);
  });

  test('filters rows by provider type', async () => {
    render(<ProviderTable />);

    // Simulate filter change (checkbox interaction)
    const checkbox = screen.getByLabelText('Online');
    fireEvent.click(checkbox);

    // Verify the filtered results
    const rows = await screen.findAllByRole('row');
    const onlineRows = providerList.data.filter((provider) =>
      provider['provider-type'].includes('online')
    );
    expect(rows).toHaveLength(onlineRows.length + 1); // +1 for header
  });
});
